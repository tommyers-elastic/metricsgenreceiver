package metricsgenreceiver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	lifecycleGoldenSHA256 = "86f8780822ed9d05e0ce7878e086bf6feb5de6a29f5c9dfabe08b2763bce9d90"
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
	lifecycle := &instanceLifecycle{}
	lifecycle.snapshotBaseline(scn, lifecycleStart, lifecycleStart)

	// Neither value is representable as a float64.
	emitted := pmetric.NewNumberDataPoint()
	emitted.SetIntValue(big)
	lifecycle.baseline.subtract(emitted, 0)
	assert.Equal(t, int64(0), emitted.IntValue())
	emitted.SetIntValue(big + 2)
	lifecycle.baseline.subtract(emitted, 0)
	assert.Equal(t, int64(2), emitted.IntValue())
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
