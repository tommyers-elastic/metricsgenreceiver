package metricsgenreceiver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const (
	lifecycleTemplatePath = "testdata/lifecycle-template"
	// counterResetTemplatePath has cumulative monotonic sums with every kind of hint.
	counterResetTemplatePath = "testdata/counter-reset-template"
	// lifecycleGoldenSHA256 is the SHA-256 of the output of TestInstanceLifecycleDeterministic.
	// The fixture only uses values that are computed identically on arm64 and amd64.
	lifecycleGoldenSHA256 = "35512ee77d73ced69f4f61be7349503a60b33af8d2a5726e60411ae9c55edf8b"
)

var (
	lifecycleStart = time.Date(2024, 12, 17, 0, 0, 0, 0, time.UTC)
	// cumulativeCounters are the cumulative monotonic series of the lifecycle template.
	cumulativeCounters = []string{"system.cpu.time", "system.network.io", "system.disk.operations"}
	// notReset are the series of the lifecycle template that a restart does not reset.
	notReset = []string{"system.processes.count", "system.memory.usage", "system.cpu.utilization"}
)

// lifecycleConfig runs the lifecycle template for ten 30s intervals with the given instances.
func lifecycleConfig(instances ...InstanceCfg) *Config {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.StartTime = lifecycleStart
	cfg.EndTime = cfg.StartTime.Add(5 * time.Minute)
	cfg.Interval = 30 * time.Second
	cfg.Seed = 11
	cfg.Scenarios = []ScenarioCfg{{Path: lifecycleTemplatePath, Instances: instances}}
	return cfg
}

func lifecycleInstance(host string, modify func(*InstanceCfg)) InstanceCfg {
	inst := InstanceCfg{ResourceAttributes: map[string]any{"host.name": host}}
	if modify != nil {
		modify(&inst)
	}
	return inst
}

func at(offset time.Duration) *time.Time {
	return timePtr(lifecycleStart.Add(offset))
}

func TestInstanceLifetime(t *testing.T) {
	cfg := lifecycleConfig(
		lifecycleInstance("always", nil),
		lifecycleInstance("late", func(c *InstanceCfg) { c.StartTime = at(time.Minute) }),
		lifecycleInstance("early", func(c *InstanceCfg) { c.EndTime = at(2 * time.Minute) }),
		lifecycleInstance("both", func(c *InstanceCfg) { c.StartTime = at(time.Minute); c.EndTime = at(2 * time.Minute) }),
		// Sample times are tick + 10s: 0:40 is the first included, 1:10 is excluded.
		lifecycleInstance("offset", func(c *InstanceCfg) {
			c.IntervalOffset = durationPtr(10 * time.Second)
			c.StartTime = at(40 * time.Second)
			c.EndTime = at(70 * time.Second)
		}),
	)
	require.NoError(t, cfg.Validate())
	byHost := batchesByHost(t, runReceiverToCompletion(t, cfg))

	tests := []struct {
		host        string
		samples     int
		first, last time.Duration
	}{
		{host: "always", samples: 10, first: 0, last: 270 * time.Second},
		{host: "late", samples: 8, first: 60 * time.Second, last: 270 * time.Second},
		{host: "early", samples: 4, first: 0, last: 90 * time.Second},
		{host: "both", samples: 2, first: 60 * time.Second, last: 90 * time.Second},
		{host: "offset", samples: 1, first: 40 * time.Second, last: 40 * time.Second},
	}
	for _, tc := range tests {
		batches := byHost[tc.host]
		require.Len(t, batches, tc.samples, tc.host)
		assert.Equal(t, lifecycleStart.Add(tc.first), batchTimestamp(t, batches[0]), tc.host)
		assert.Equal(t, lifecycleStart.Add(tc.last), batchTimestamp(t, batches[len(batches)-1]), tc.host)
	}

	// Instances that start inside the window report start_time as their start timestamp and count
	// from zero; the others are unchanged.
	assert.Equal(t, lifecycleStart.Add(time.Minute), batchStartTimestamp(t, byHost["late"][0]))
	assert.Equal(t, lifecycleStart, batchStartTimestamp(t, byHost["early"][0]))
	for _, name := range cumulativeCounters {
		assert.Zero(t, metricValue(t, byHost["late"][0], name), name)
		assert.NotZero(t, metricValue(t, byHost["early"][0], name), name)
	}
}

func TestInstanceRestart(t *testing.T) {
	newConfig := func(restarts ...time.Time) *Config {
		return lifecycleConfig(
			lifecycleInstance("restarting", func(c *InstanceCfg) {
				c.IntervalOffset = durationPtr(5 * time.Second)
				c.StartTimestamp = timePtr(lifecycleStart.Add(-time.Hour))
				c.Restarts = restarts
			}),
			// Shares the template with the restarting instance and started before the window.
			lifecycleInstance("steady", func(c *InstanceCfg) {
				c.StartTimestamp = timePtr(lifecycleStart.Add(-2 * time.Hour))
			}),
		)
	}
	firstRestart := lifecycleStart.Add(125 * time.Second)
	secondRestart := lifecycleStart.Add(215 * time.Second)
	cfg := newConfig(firstRestart, secondRestart)
	require.NoError(t, cfg.Validate())
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	require.Len(t, rcv.scenarios, 1, "both instances share one template")

	restarted := batchesByHost(t, runReceiverToCompletion(t, cfg))
	reference := batchesByHost(t, runReceiverToCompletion(t, newConfig()))
	require.Len(t, restarted["restarting"], 10)
	require.Len(t, restarted["steady"], 10)

	// The instance that does not restart is byte-identical to the run without restarts.
	assert.Equal(t, marshalMetricsOutput(t, reference["steady"]).String(), marshalMetricsOutput(t, restarted["steady"]).String())

	// Sample times are tick + 5s; the restarts are at the samples 4 (2:05) and 7 (3:35).
	wantStart := func(sample int) time.Time {
		switch {
		case sample >= 7:
			return secondRestart
		case sample >= 4:
			return firstRestart
		default:
			return lifecycleStart.Add(-time.Hour)
		}
	}
	for sample := 0; sample < 10; sample++ {
		got := restarted["restarting"][sample]
		want := reference["restarting"][sample]
		assert.Equal(t, wantStart(sample), batchStartTimestamp(t, got), "sample %d", sample)
		assert.Equal(t, batchTimestamp(t, want), batchTimestamp(t, got), "sample %d", sample)
		if sample < 4 {
			// Before the first restart, the output is unchanged.
			assert.Equal(t, marshalMetricsOutput(t, []pmetric.Metrics{want}).String(), marshalMetricsOutput(t, []pmetric.Metrics{got}).String(), "sample %d", sample)
			continue
		}
		for _, name := range notReset {
			assert.Equal(t, metricValue(t, want, name), metricValue(t, got, name), "sample %d %s", sample, name)
		}
		for _, name := range cumulativeCounters {
			value := metricValue(t, got, name)
			if sample == 4 || sample == 7 {
				assert.Zero(t, value, "first sample after a restart: sample %d %s", sample, name)
			} else {
				previous := metricValue(t, restarted["restarting"][sample-1], name)
				assert.GreaterOrEqual(t, value, previous, "sample %d %s", sample, name)
				assert.Less(t, value, metricValue(t, want, name), "sample %d %s", sample, name)
			}
		}
	}
	// The counters do increase after a restart.
	for _, name := range cumulativeCounters {
		assert.Positive(t, metricValue(t, restarted["restarting"][6], name), name)
	}
}

func TestInstanceRestartDoesNotResetDeltaSums(t *testing.T) {
	newConfig := func(restarts ...time.Time) *Config {
		cfg := lifecycleConfig(lifecycleInstance("a", func(c *InstanceCfg) { c.Restarts = restarts }))
		cfg.Scenarios[0].TemporalityOverride = "delta"
		return cfg
	}
	restart := lifecycleStart.Add(2 * time.Minute)
	restarted := runReceiverToCompletion(t, newConfig(restart))
	reference := runReceiverToCompletion(t, newConfig())
	require.Len(t, restarted, 10)
	for i := range restarted {
		for _, name := range append(append([]string{}, cumulativeCounters...), notReset...) {
			assert.Equal(t, metricValue(t, reference[i], name), metricValue(t, restarted[i], name), "sample %d %s", i, name)
		}
		if i >= 4 {
			assert.Equal(t, restart, batchStartTimestamp(t, restarted[i]), "sample %d", i)
		} else {
			assert.Equal(t, lifecycleStart, batchStartTimestamp(t, restarted[i]), "sample %d", i)
		}
	}
}

func TestInstanceRestartResetsOnlyCounters(t *testing.T) {
	newConfig := func(restarts ...time.Time) *Config {
		cfg := lifecycleConfig(lifecycleInstance("a", func(c *InstanceCfg) { c.Restarts = restarts }))
		cfg.Scenarios[0].Path = counterResetTemplatePath
		return cfg
	}
	restart := lifecycleStart.Add(2 * time.Minute)
	cfg := newConfig(restart)
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	require.Len(t, rcv.scenarios, 1)
	assert.Equal(t, baselineSize{ints: 2, doubles: 1}, rcv.scenarios[0].resetSlotCounts)

	restarted := runReceiverToCompletion(t, cfg)
	reference := runReceiverToCompletion(t, newConfig())
	require.Len(t, restarted, 10)
	require.Len(t, reference, 10)
	// Cumulative monotonic sums without a hint or with a counter hint count from zero after the
	// restart at sample 4; those with other hints are identical to the run without the restart.
	counters := []string{"requests.unhinted", "bytes.steady", "errors.sparse"}
	others := []string{"sessions.current", "load.slow", "limit.constant"}
	for i := range restarted {
		for _, name := range others {
			assert.Equal(t, metricValue(t, reference[i], name), metricValue(t, restarted[i], name), "sample %d %s", i, name)
		}
		for _, name := range counters {
			value := metricValue(t, restarted[i], name)
			switch {
			case i < 4:
				assert.Equal(t, metricValue(t, reference[i], name), value, "sample %d %s", i, name)
			case i == 4:
				assert.Zero(t, value, "first sample after the restart: %s", name)
			default:
				assert.GreaterOrEqual(t, value, metricValue(t, restarted[i-1], name), "sample %d %s", i, name)
			}
		}
	}
	for _, name := range []string{"requests.unhinted", "bytes.steady"} {
		assert.Positive(t, metricValue(t, restarted[9], name), name)
	}
}

func TestCounterBaselineKeepsIntsExact(t *testing.T) {
	const big = int64(1)<<53 + 1
	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("big.counter")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.DataPoints().AppendEmpty().SetIntValue(big)

	scn := &Scenario{metricsTemplate: &metrics}
	scn.resetSlots, scn.resetSlotCounts = resetSlotsFor(&metrics, nil)
	require.Equal(t, []int{0}, scn.resetSlots)
	scn.previousResetValues = newCounterBaseline(scn.resetSlotCounts)
	previous := pmetric.NewNumberDataPoint()
	previous.SetIntValue(big - 10)
	scn.previousResetValues.record(previous, 0)

	// A sample at the start subtracts the whole template value. Neither value is representable
	// as a float64.
	lifecycle := &instanceLifecycle{}
	lifecycle.snapshotBaseline(scn, lifecycleStart, lifecycleStart, lifecycleStart, 30*time.Second)
	emitted := pmetric.NewNumberDataPoint()
	emitted.SetIntValue(big)
	lifecycle.baseline.subtract(emitted, 0)
	assert.Equal(t, int64(0), emitted.IntValue())
	emitted.SetIntValue(big + 2)
	lifecycle.baseline.subtract(emitted, 0)
	assert.Equal(t, int64(2), emitted.IntValue())

	// A sample 12s after the start keeps 12/30 of the latest increase of 10, rounded.
	lifecycle.snapshotBaseline(scn, lifecycleStart, lifecycleStart.Add(30*time.Second), lifecycleStart.Add(12*time.Second), 30*time.Second)
	emitted.SetIntValue(big)
	lifecycle.baseline.subtract(emitted, 0)
	assert.Equal(t, int64(4), emitted.IntValue())
	assert.Equal(t, lifecycleStart.Add(18*time.Second), lifecycle.variationStart)
}

func TestInstanceBaselineMemory(t *testing.T) {
	cfg := lifecycleConfig(
		lifecycleInstance("restarted", func(c *InstanceCfg) { c.Restarts = []time.Time{*at(time.Minute)} }),
		lifecycleInstance("ended", func(c *InstanceCfg) { c.Restarts = []time.Time{*at(time.Minute)}; c.EndTime = at(2 * time.Minute) }),
		lifecycleInstance("before-window", func(c *InstanceCfg) { c.StartTime = at(-time.Hour) }),
		lifecycleInstance("plain", nil),
	)
	require.NoError(t, cfg.Validate())
	sink := new(consumertest.MetricsSink)
	rcv, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(typ), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), nil))
	waitForCompletion(rcv)
	require.NoError(t, rcv.Shutdown(context.Background()))

	scn := rcv.(*MetricsGenReceiver).scenarios[0]
	assert.Equal(t, baselineSize{ints: 2, doubles: 1}, scn.resetSlotCounts)
	instances := scn.instances
	require.Len(t, instances, 4)
	require.NotNil(t, instances[0].lifecycle.baseline)
	assert.Len(t, instances[0].lifecycle.baseline.ints, 2, "one value per int counter series")
	assert.Len(t, instances[0].lifecycle.baseline.doubles, 1, "one value per double counter series")
	assert.Nil(t, instances[1].lifecycle.baseline, "dropped once end_time has passed")
	assert.Nil(t, instances[2].lifecycle.baseline, "no baseline for a start before the window")
	assert.Nil(t, instances[3].lifecycle, "no lifecycle without start_time, end_time or restarts")
}

func TestInstanceLifecycleDeterministic(t *testing.T) {
	newConfig := func() *Config {
		cfg := lifecycleConfig(
			lifecycleInstance("a", func(c *InstanceCfg) { c.Restarts = []time.Time{*at(100 * time.Second)} }),
			lifecycleInstance("b", func(c *InstanceCfg) { c.StartTime = at(70 * time.Second); c.EndTime = at(4 * time.Minute) }),
			lifecycleInstance("c", func(c *InstanceCfg) {
				c.StartTimestamp = at(-time.Hour)
				c.EndTime = at(3 * time.Minute)
			}),
		)
		cfg.StaggerInstances = true
		cfg.IntervalJitterStdDev = 5 * time.Millisecond
		return cfg
	}
	first := marshalMetricsOutput(t, runReceiverToCompletion(t, newConfig()))
	second := marshalMetricsOutput(t, runReceiverToCompletion(t, newConfig()))
	require.Equal(t, first.String(), second.String())
	sum := sha256.Sum256(first.Bytes())
	assert.Equal(t, lifecycleGoldenSHA256, hex.EncodeToString(sum[:]))
}

func TestInstanceLifecycleValidate(t *testing.T) {
	tests := []struct {
		name      string
		instance  InstanceCfg
		wantError string
	}{
		{
			name: "start timestamp inside the window before a late start",
			instance: InstanceCfg{
				StartTimestamp: at(time.Minute), StartTime: at(2 * time.Minute), EndTime: at(4 * time.Minute),
				Restarts: []time.Time{*at(3 * time.Minute)},
			},
		},
		{
			name:     "restarts without start",
			instance: InstanceCfg{Restarts: []time.Time{*at(-time.Minute), *at(time.Minute)}},
		},
		{
			name:      "start time equal to end time",
			instance:  InstanceCfg{StartTime: at(time.Minute), EndTime: at(time.Minute)},
			wantError: "instances[0]: start_time (2024-12-17T00:01:00Z) must be before end_time (2024-12-17T00:01:00Z)",
		},
		{
			name:      "start timestamp after start time",
			instance:  InstanceCfg{StartTimestamp: at(2 * time.Minute), StartTime: at(time.Minute)},
			wantError: "instances[0]: start_timestamp (2024-12-17T00:02:00Z) must not be after start_time (2024-12-17T00:01:00Z)",
		},
		{
			name:      "start timestamp after the receiver's start time without start time",
			instance:  InstanceCfg{StartTimestamp: at(time.Minute), EndTime: at(2 * time.Minute)},
			wantError: "instances[0]: start_timestamp (2024-12-17T00:01:00Z) must not be after the receiver's start_time (2024-12-17T00:00:00Z)",
		},
		{
			name:      "restarts not ascending",
			instance:  InstanceCfg{Restarts: []time.Time{*at(2 * time.Minute), *at(2 * time.Minute)}},
			wantError: "instances[0]: restarts[1] (2024-12-17T00:02:00Z) must be after restarts[0] (2024-12-17T00:02:00Z)",
		},
		{
			name:      "restart at start timestamp",
			instance:  InstanceCfg{StartTimestamp: at(-time.Minute), Restarts: []time.Time{*at(-time.Minute)}},
			wantError: "instances[0]: restarts[0] (2024-12-16T23:59:00Z) must be after start_timestamp (2024-12-16T23:59:00Z)",
		},
		{
			name:      "restart at start time",
			instance:  InstanceCfg{StartTime: at(time.Minute), Restarts: []time.Time{*at(time.Minute)}},
			wantError: "instances[0]: restarts[0] (2024-12-17T00:01:00Z) must be after start_time (2024-12-17T00:01:00Z)",
		},
		{
			name:      "restart before start time",
			instance:  InstanceCfg{StartTime: at(time.Minute), Restarts: []time.Time{*at(30 * time.Second)}},
			wantError: "instances[0]: restarts[0] (2024-12-17T00:00:30Z) must be after start_time (2024-12-17T00:01:00Z)",
		},
		{
			name:      "restart at end time",
			instance:  InstanceCfg{EndTime: at(time.Minute), Restarts: []time.Time{*at(time.Minute)}},
			wantError: "instances[0]: restarts[0] (2024-12-17T00:01:00Z) must be before end_time (2024-12-17T00:01:00Z)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := lifecycleConfig(tc.instance)
			err := cfg.Validate()
			if tc.wantError == "" {
				require.NoError(t, err)
				_, err = newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantError)
			}
		})
	}

	t.Run("file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "instances.ndjson")
		require.NoError(t, os.WriteFile(path, []byte(`{"start_time":"2024-12-17T00:02:00Z","end_time":"2024-12-17T00:01:00Z"}`+"\n"), 0o600))
		cfg := lifecycleConfig()
		cfg.Scenarios[0].Instances = nil
		cfg.Scenarios[0].InstancesPath = path
		_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
		require.EqualError(t, err, path+":1: start_time (2024-12-17T00:02:00Z) must be before end_time (2024-12-17T00:01:00Z)")
	})
}

func TestLoadConfigInstanceLifecycle(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(`metricsgen:
  interval: 30s
  scenarios:
  - path: testdata/lifecycle-template
    instances:
    - start_time: "2024-12-17T00:01:00Z"
      end_time: "2024-12-17T00:04:00Z"
      restarts: ["2024-12-17T00:02:00Z", "2024-12-17T00:03:00Z"]
`), 0o600))
	ndjsonPath := filepath.Join(dir, "instances.ndjson")
	require.NoError(t, os.WriteFile(ndjsonPath, []byte(
		`{"start_time":"2024-12-17T00:01:00Z","end_time":"2024-12-17T00:04:00Z","restarts":["2024-12-17T00:02:00Z","2024-12-17T00:03:00Z"]}`+"\n"), 0o600))

	cm, err := confmaptest.LoadConf(yamlPath)
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	sub, err := cm.Sub("metricsgen")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	want := []InstanceCfg{{
		StartTime: at(time.Minute),
		EndTime:   at(4 * time.Minute),
		Restarts:  []time.Time{*at(2 * time.Minute), *at(3 * time.Minute)},
	}}
	assert.Equal(t, want, cfg.Scenarios[0].Instances)
	fromFile, err := loadInstancesFile(ndjsonPath, cfg.Interval)
	require.NoError(t, err)
	assert.Equal(t, want, instanceCfgs(fromFile))
}

// batchesByHost groups the emitted batches by host.name, in emission order.
func batchesByHost(t *testing.T, allMetrics []pmetric.Metrics) map[string][]pmetric.Metrics {
	t.Helper()
	byHost := map[string][]pmetric.Metrics{}
	for _, md := range allMetrics {
		host := resourceHostName(t, md)
		byHost[host] = append(byHost[host], md)
	}
	return byHost
}

func batchStartTimestamp(t *testing.T, md pmetric.Metrics) time.Time {
	t.Helper()
	var ts pcommon.Timestamp
	dp.ForEachDataPoint(&md, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
		if idx == 0 {
			ts = dataPoint.StartTimestamp()
		}
		require.Equal(t, ts, dataPoint.StartTimestamp())
	})
	return ts.AsTime()
}

// metricValue returns the value of the single number data point of the named metric.
func metricValue(t *testing.T, md pmetric.Metrics, name string) float64 {
	t.Helper()
	var values []float64
	dp.ForEachDataPoint(&md, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		if m.Name() == name {
			values = append(values, numberValue(dataPoint.(pmetric.NumberDataPoint)))
		}
	})
	require.Len(t, values, 1, name)
	return values[0]
}

func numberValue(v pmetric.NumberDataPoint) float64 {
	if v.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(v.IntValue())
	}
	return v.DoubleValue()
}

func TestFirstSampleAfterStartCountsFromStart(t *testing.T) {
	newConfig := func(withTiming bool) *Config {
		instances := []InstanceCfg{
			// The first sample is at 1:30, 20s after the start.
			lifecycleInstance("late", func(c *InstanceCfg) { c.StartTime = at(70 * time.Second) }),
			// The first samples after the restarts are at 2:00 and 4:00, 20s and 25s after them.
			lifecycleInstance("restarting", func(c *InstanceCfg) {
				c.Restarts = []time.Time{*at(100 * time.Second), *at(215 * time.Second)}
			}),
			// The first sample is at the start.
			lifecycleInstance("exact", func(c *InstanceCfg) { c.StartTime = at(time.Minute) }),
			// The first sample is at the start time, 60s after the start timestamp.
			lifecycleInstance("stamped", func(c *InstanceCfg) {
				c.StartTimestamp = at(30 * time.Second)
				c.StartTime = at(90 * time.Second)
			}),
			// Sample times are tick + 10s: the first is at 1:10, 25s after the start.
			lifecycleInstance("offset", func(c *InstanceCfg) {
				c.IntervalOffset = durationPtr(10 * time.Second)
				c.StartTime = at(45 * time.Second)
			}),
		}
		if !withTiming {
			// Timing fields are not part of an instance's key: the same instances without them
			// emit the template values the instances above count from.
			for i := range instances {
				instances[i] = lifecycleInstance(instances[i].ResourceAttributes["host.name"].(string), nil)
			}
		}
		cfg := lifecycleConfig(instances...)
		cfg.Scenarios[0].Path = seededRateTemplatePath
		return cfg
	}
	cfg := newConfig(true)
	require.NoError(t, cfg.Validate())
	started := batchesByHost(t, runReceiverToCompletion(t, cfg))
	reference := batchesByHost(t, runReceiverToCompletion(t, newConfig(false)))

	wantFirst := map[string][]time.Duration{
		"late":       {20 * time.Second},
		"restarting": {20 * time.Second, 25 * time.Second},
		"exact":      {0},
		"stamped":    {60 * time.Second},
		"offset":     {25 * time.Second},
	}
	for host, elapsed := range wantFirst {
		require.Len(t, reference[host], 10, host)
		var firsts []time.Duration
		var previous map[string]float64
		var previousStart time.Time
		for _, batch := range started[host] {
			start := batchStartTimestamp(t, batch)
			sampleTime := batchTimestamp(t, batch)
			tick := int(sampleTime.Sub(lifecycleStart) / cfg.Interval)
			values := seriesValues(t, batch)
			current := seriesValues(t, reference[host][tick])
			if !start.After(lifecycleStart) {
				// Started before the window: as without the timing fields.
				assert.Equal(t, current, values, "%s %s", host, sampleTime)
				continue
			}
			if tick == 0 {
				continue
			}
			before := seriesValues(t, reference[host][tick-1])
			if !start.Equal(previousStart) {
				// The first sample after a (re)start reads the latest increase times the time
				// since the start in intervals.
				firsts = append(firsts, sampleTime.Sub(start))
				fraction := float64(sampleTime.Sub(start)) / float64(cfg.Interval)
				for _, series := range seededRateSeries {
					want := fraction * (current[series.key] - before[series.key])
					assert.InDelta(t, want, values[series.key], 4, "%s %s %s", host, sampleTime, series.key)
					if fraction == 0 {
						assert.Zero(t, values[series.key], "%s %s %s", host, sampleTime, series.key)
					}
				}
				if fraction > 0 {
					assert.Positive(t, values["system.cpu.time|idle"], host)
					assert.Positive(t, values["system.network.io|eth0"], host)
				}
			} else {
				// Afterwards, the counters increase as without the timing fields.
				for _, series := range seededRateSeries {
					assert.GreaterOrEqual(t, values[series.key], previous[series.key], "%s %s %s", host, sampleTime, series.key)
					want := current[series.key] - before[series.key]
					assert.InDelta(t, want, values[series.key]-previous[series.key], 2, "%s %s %s", host, sampleTime, series.key)
				}
			}
			previous = values
			previousStart = start
		}
		assert.Equal(t, elapsed, firsts, host)
	}
}

func TestFirstSampleAfterRestartWithCounterVariation(t *testing.T) {
	cfg := lifecycleConfig(lifecycleInstance("restarting", func(c *InstanceCfg) {
		c.Restarts = []time.Time{*at(100 * time.Second)}
	}))
	batches := runReceiverToCompletion(t, cfg)
	require.Len(t, batches, 10)
	// The same instance without the restart (restarts are not part of the key).
	reference := runReceiverToCompletion(t, lifecycleConfig(lifecycleInstance("restarting", nil)))
	require.Len(t, reference, 10)
	// The restart is 20s before the sample of tick 4.
	assert.Equal(t, lifecycleStart.Add(100*time.Second), batchStartTimestamp(t, batches[4]))
	const fraction = 2.0 / 3
	// The first sample and the instance's increase over the restart's interval, times 2/3, carry
	// the same multiple of the template's increase. They differ in the time-dependent extra rate
	// of the counter variation, which is between 0.2 and 1.8 times 20% of median_monotonic_sum per
	// interval and is taken over different times, so by at most 1.6 * 0.2 * median_monotonic_sum
	// * 2/3, plus the rounding of three values.
	tolerance := float64(1.6*0.2*float64(cfg.Distribution.MedianMonotonicSum)*fraction) + 2
	for _, name := range cumulativeCounters {
		first := metricValue(t, batches[4], name)
		assert.Positive(t, first, name)
		assert.Less(t, first, metricValue(t, batches[3], name), name)
		rate := metricValue(t, reference[4], name) - metricValue(t, reference[3], name)
		assert.InDelta(t, rate*fraction, first, tolerance, name)
		for i := 5; i < len(batches); i++ {
			assert.GreaterOrEqual(t, metricValue(t, batches[i], name), metricValue(t, batches[i-1], name), "sample %d %s", i, name)
		}
	}
}

func TestFirstSampleInFirstInterval(t *testing.T) {
	newConfig := func(withTiming bool) *Config {
		inst := lifecycleInstance("first", nil)
		if withTiming {
			// The first sample is on tick 0, at 0:20, 10s after the start.
			inst.IntervalOffset = durationPtr(20 * time.Second)
			inst.StartTime = at(10 * time.Second)
		}
		cfg := lifecycleConfig(inst)
		cfg.Scenarios[0].Path = seededRateTemplatePath
		return cfg
	}
	cfg := newConfig(true)
	require.NoError(t, cfg.Validate())
	batches := runReceiverToCompletion(t, cfg)
	require.Len(t, batches, 10)
	assert.Equal(t, lifecycleStart.Add(20*time.Second), batchTimestamp(t, batches[0]))
	assert.Equal(t, lifecycleStart.Add(10*time.Second), batchStartTimestamp(t, batches[0]))

	// The template's seeds and values at tick 0, and the instance without the timing fields (the
	// same key) at tick 0, which is the template value times a fixed factor per series.
	rcv, err := newMetricsGenReceiver(newConfig(false), receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	sink := new(consumertest.MetricsSink)
	rcv.nextMetrics = sink
	template := *rcv.scenarios[0].metricsTemplate
	seeds := seriesValues(t, template)
	rcv.produceMetrics(context.Background(), lifecycleStart)
	tick0 := seriesValues(t, template)
	require.Len(t, sink.AllMetrics(), 1)
	reference := seriesValues(t, sink.AllMetrics()[0])

	first := seriesValues(t, batches[0])
	for _, series := range seededRateSeries {
		factor := reference[series.key] / tick0[series.key]
		// One third of the template's first increase, times the factor; the rounding of the
		// increase and of the emitted value is less than 1.5.
		want := factor * (tick0[series.key] - seeds[series.key]) / 3
		assert.InDelta(t, want, first[series.key], 1.5, series.key)
	}
	assert.Positive(t, first["system.cpu.time|idle"])
	assert.Positive(t, first["system.network.io|eth0"])
	for i := 1; i < len(batches); i++ {
		values := seriesValues(t, batches[i])
		previous := seriesValues(t, batches[i-1])
		for _, series := range seededRateSeries {
			assert.GreaterOrEqual(t, values[series.key], previous[series.key], "sample %d %s", i, series.key)
		}
	}
}

func TestCounterBaselineClampsExtrapolation(t *testing.T) {
	intPoint := func(v int64) pmetric.NumberDataPoint {
		p := pmetric.NewNumberDataPoint()
		p.SetIntValue(v)
		return p
	}
	doublePoint := func(v float64) pmetric.NumberDataPoint {
		p := pmetric.NewNumberDataPoint()
		p.SetDoubleValue(v)
		return p
	}
	// read returns what a sample with the given value reads (before the per-instance variation)
	// with the baseline that recordSince computes from value, previous and fraction.
	read := func(value, previous pmetric.NumberDataPoint, fraction float64, later pmetric.NumberDataPoint) float64 {
		prev := newCounterBaseline(baselineSize{ints: 1, doubles: 1})
		prev.record(previous, 0)
		baseline := newCounterBaseline(baselineSize{ints: 1, doubles: 1})
		baseline.recordSince(value, prev, 0, fraction)
		emitted := pmetric.NewNumberDataPoint()
		later.CopyTo(emitted)
		baseline.subtract(emitted, 0)
		return numberValue(emitted)
	}
	const (
		maxInt = int64(math.MaxInt64)
		minInt = int64(math.MinInt64)
		capped = float64(maxAccumulated)
	)
	tests := []struct {
		name            string
		value, previous pmetric.NumberDataPoint
		fraction        float64
		want            float64
	}{
		{name: "int within the value", value: intPoint(100), previous: intPoint(90), fraction: 0.5, want: 5},
		// Not bounded by the template value: the latest rate times the elapsed time.
		{name: "int beyond the value", value: intPoint(100), previous: intPoint(0), fraction: 3, want: 300},
		{name: "int at the start", value: intPoint(100), previous: intPoint(90), fraction: 0, want: 0},
		{name: "int decreasing", value: intPoint(90), previous: intPoint(100), fraction: 2, want: 0},
		// The product is far outside the int64 range.
		{name: "int far beyond the range", value: intPoint(1 << 62), previous: intPoint(0), fraction: 1e8, want: capped},
		{name: "int at the maximum", value: intPoint(maxInt), previous: intPoint(maxInt - 1e6), fraction: 1e15, want: capped},
		// value - accumulated would wrap around below the int64 minimum.
		{name: "int near the minimum", value: intPoint(minInt + 5), previous: intPoint(minInt), fraction: 1e30, want: 5},
		{name: "double within the value", value: doublePoint(10.5), previous: doublePoint(10), fraction: 2, want: 1},
		{name: "double beyond the value", value: doublePoint(10.5), previous: doublePoint(0), fraction: 2, want: 21},
		// The product overflows to +Inf.
		{name: "double overflow", value: doublePoint(10), previous: doublePoint(-1e308), fraction: 1e10, want: capped},
		{name: "double NaN", value: doublePoint(10), previous: doublePoint(math.NaN()), fraction: 2, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, read(tc.value, tc.previous, tc.fraction, tc.value))
		})
	}

	t.Run("later samples", func(t *testing.T) {
		// A later sample reads its value minus the baseline, saturated instead of wrapping around.
		assert.Equal(t, capped+1000, read(intPoint(0), intPoint(-1), 1e30, intPoint(1000)))
		assert.Equal(t, float64(maxInt), read(intPoint(0), intPoint(-1), 1e30, intPoint(maxInt)))
		assert.Equal(t, float64(maxInt), read(intPoint(minInt+5), intPoint(minInt), 1e30, intPoint(maxInt)))
		// A huge double template value cannot carry the accumulated amount, but stays finite.
		huge := read(doublePoint(1e300), doublePoint(0), 1e10, doublePoint(1e300))
		assert.False(t, math.IsInf(huge, 0) || math.IsNaN(huge))
		assert.GreaterOrEqual(t, huge, 0.0)
		assert.LessOrEqual(t, huge, capped)
	})

	// Through snapshotBaseline: a start about eleven years before the sample at 1s intervals.
	t.Run("snapshot", func(t *testing.T) {
		metrics := pmetric.NewMetrics()
		m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("big.counter")
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		sum.DataPoints().AppendEmpty().SetIntValue(1 << 50)
		scn := &Scenario{metricsTemplate: &metrics}
		scn.resetSlots, scn.resetSlotCounts = resetSlotsFor(&metrics, nil)
		scn.previousResetValues = newCounterBaseline(scn.resetSlotCounts)
		scn.previousResetValues.record(intPoint(1<<50-1<<40), 0)
		lifecycle := &instanceLifecycle{}
		sampleTime := lifecycleStart.Add(100000 * time.Hour)
		lifecycle.snapshotBaseline(scn, lifecycleStart, sampleTime, sampleTime, time.Second)
		emitted := intPoint(1 << 50)
		lifecycle.baseline.subtract(emitted, 0)
		assert.Equal(t, int64(maxAccumulated), emitted.IntValue())
		later := intPoint(1 << 51)
		lifecycle.baseline.subtract(later, 0)
		assert.Equal(t, int64(maxAccumulated+1<<50), later.IntValue())
	})
}

// counterSeriesValues returns the values of the cumulative monotonic sums, keyed by metric name
// and all data point attributes.
func counterSeriesValues(md pmetric.Metrics) map[string]float64 {
	values := map[string]float64{}
	dp.ForEachDataPoint(&md, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		if !isCumulativeMonotonicSum(m) {
			return
		}
		key := m.Name()
		dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			key += "|" + k + "=" + v.AsString()
			return true
		})
		values[key] = numberValue(dataPoint.(pmetric.NumberDataPoint))
	})
	return values
}

func TestFirstSampleExtrapolatesBeyondTemplateValue(t *testing.T) {
	// A start_timestamp 10s after the receiver's start and a start_time 4m after it: the first
	// sample is at 4:00 (tick 8), 230s after the start.
	newConfig := func(withTiming bool) *Config {
		inst := lifecycleInstance("late", nil)
		if withTiming {
			inst.StartTimestamp = at(10 * time.Second)
			inst.StartTime = at(4 * time.Minute)
		}
		cfg := lifecycleConfig(inst)
		cfg.Scenarios[0].Path = "builtin/hostmetrics"
		return cfg
	}
	const firstTick = 8
	fraction := float64(230*time.Second) / float64(30*time.Second)

	// Drive the receiver tick by tick to read the template before and after the first tick and
	// the baseline the instance takes on it.
	cfg := newConfig(true)
	require.NoError(t, cfg.Validate())
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	sink := new(consumertest.MetricsSink)
	rcv.nextMetrics = sink
	scn := &rcv.scenarios[0]
	var before map[string]float64
	for tick := 0; tick <= firstTick; tick++ {
		before = counterSeriesValues(*scn.metricsTemplate)
		rcv.produceMetrics(context.Background(), lifecycleStart.Add(time.Duration(tick)*cfg.Interval))
	}
	current := counterSeriesValues(*scn.metricsTemplate)
	require.Len(t, sink.AllMetrics(), 1)
	baseline := scn.instances[0].lifecycle.baseline
	require.NotNil(t, baseline)

	// What each reset series accumulated: the latest increase times the elapsed time in
	// intervals, whatever the template's value at the tick.
	accumulated := map[string]float64{}
	beyond := 0
	dp.ForEachDataPoint(scn.metricsTemplate, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		slot := scn.resetSlots[idx]
		if slot < 0 {
			return
		}
		key := m.Name()
		dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			key += "|" + k + "=" + v.AsString()
			return true
		})
		want := fraction * (current[key] - before[key])
		point := dataPoint.(pmetric.NumberDataPoint)
		if point.ValueType() == pmetric.NumberDataPointValueTypeInt {
			accumulated[key] = float64(point.IntValue() - baseline.ints[slot])
			assert.Equal(t, math.Round(want), accumulated[key], key)
		} else {
			accumulated[key] = point.DoubleValue() - baseline.doubles[slot]
			assert.InDelta(t, want, accumulated[key], 1e-6*math.Max(1, want), key)
		}
		if accumulated[key] > current[key] {
			beyond++
		}
	})
	require.NotEmpty(t, accumulated)
	const errorsKey = "system.network.errors|device=eth0|direction=receive"
	assert.Greater(t, accumulated[errorsKey], current[errorsKey])
	assert.Positive(t, beyond)

	// In the output, the first sample of such a series reads more than the same instance without
	// the timing fields at the same tick: the template value at the tick with the same per-instance
	// multiplier and at least the same time-dependent extra (it runs over a longer time there).
	batches := runReceiverToCompletion(t, cfg)
	reference := runReceiverToCompletion(t, newConfig(false))
	require.Len(t, batches, 2)
	require.Len(t, reference, 10)
	require.Equal(t, lifecycleStart.Add(4*time.Minute), batchTimestamp(t, batches[0]))
	require.Equal(t, lifecycleStart.Add(10*time.Second), batchStartTimestamp(t, batches[0]))
	first := counterSeriesValues(batches[0])
	referenceFirst := counterSeriesValues(reference[firstTick])
	for key, amount := range accumulated {
		if amount > current[key] {
			assert.Greater(t, first[key], referenceFirst[key], key)
		}
	}
	// Afterwards the counters keep increasing.
	second := counterSeriesValues(batches[1])
	for key := range accumulated {
		assert.GreaterOrEqual(t, second[key], first[key], key)
	}
}
