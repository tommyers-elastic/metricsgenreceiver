package metricsgenreceiver

import (
	"context"
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

func durationPtr(d time.Duration) *time.Duration {
	return &d
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func TestStaggerOffsetIsPinned(t *testing.T) {
	cfg := &Config{Seed: 7, Interval: 30 * time.Second, StaggerInstances: true}
	// Integer arithmetic only: the same on every architecture.
	want := [][]time.Duration{
		{14629033336, 29626417888, 15919644772, 13375045472},
		{26249684972, 7539200646, 29653123811, 2381615036},
	}
	for scenarioIndex, offsets := range want {
		for instanceID, offset := range offsets {
			got := staggerOffset(cfg, scenarioIndex, instanceID)
			assert.Equal(t, offset, got, "scenario %d instance %d", scenarioIndex, instanceID)
			assert.GreaterOrEqual(t, got, time.Duration(0))
			assert.Less(t, got, cfg.Interval)
		}
	}

	cfg.Seed = 8
	assert.NotEqual(t, want[0][0], staggerOffset(cfg, 0, 0))
	cfg.StaggerInstances = false
	assert.Equal(t, time.Duration(0), staggerOffset(cfg, 0, 1))
}

func TestStaggerInstancesExplicitInstances(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.StaggerInstances = true
	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 12)

	timestamps := instanceTimestamps(t, allMetrics)
	offsets := map[string]time.Duration{}
	for id, host := range []string{"web-1", "web-2", "db-1", "cache-1"} {
		offset := staggerOffset(cfg, 0, id)
		offsets[host] = offset
		require.Len(t, timestamps[host], 3, host)
		for k, ts := range timestamps[host] {
			assert.Equal(t, cfg.StartTime.Add(time.Duration(k)*cfg.Interval+offset), ts, host)
		}
	}
	assert.Len(t, uniqueDurations(offsets), 4, "offsets differ between instances")

	// Only the timestamps change.
	unstaggered := runReceiverToCompletion(t, explicitInstancesFileConfig())
	assert.Equal(t, marshalWithoutTimestamps(t, unstaggered), marshalWithoutTimestamps(t, allMetrics))
	assert.NotEqual(t, marshalMetricsOutput(t, unstaggered).String(), marshalMetricsOutput(t, allMetrics).String())

	// Deterministic.
	assert.Equal(t, marshalMetricsOutput(t, allMetrics).String(), marshalMetricsOutput(t, runReceiverToCompletion(t, cfg)).String())
}

func TestStaggerInstancesGeneratedInstances(t *testing.T) {
	newConfig := func(stagger bool) *Config {
		cfg := goldenMatrixBaseConfig()
		cfg.IntervalJitterStdDev = 5 * time.Millisecond
		cfg.InstanceOffset = 3
		cfg.StaggerInstances = stagger
		cfg.Scenarios = []ScenarioCfg{{Path: "builtin/hostmetrics", Scale: 4}}
		return cfg
	}
	cfg := newConfig(true)
	staggered := runReceiverToCompletion(t, cfg)
	unstaggered := runReceiverToCompletion(t, newConfig(false))
	require.Len(t, staggered, 40)

	for i, md := range staggered {
		tick := cfg.StartTime.Add(time.Duration(i/4) * cfg.Interval)
		offset := staggerOffset(cfg, 0, i%4+int(cfg.InstanceOffset))
		unstaggeredTimestamp := batchTimestamp(t, unstaggered[i])
		// The jitter is drawn exactly as without staggering.
		assert.Equal(t, unstaggeredTimestamp.Add(offset), batchTimestamp(t, md), "batch %d", i)
		assert.True(t, !unstaggeredTimestamp.Before(tick) && unstaggeredTimestamp.Before(tick.Add(cfg.Interval)), "batch %d", i)
	}
	assert.Equal(t, marshalWithoutTimestamps(t, unstaggered), marshalWithoutTimestamps(t, staggered))
}

func TestStaggerInstancesChurnReplacements(t *testing.T) {
	cfg := goldenMatrixBaseConfig()
	cfg.StaggerInstances = true
	cfg.Scenarios = []ScenarioCfg{
		{Path: "builtin/hostmetrics", Scale: 1},
		{Path: "builtin/hostmetrics", Scale: 2, Churn: &ChurnCfg{SamplesPerSeries: 1}},
	}
	require.NoError(t, cfg.Validate())
	sink := new(consumertest.MetricsSink)
	rcv, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(typ), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), nil))
	waitForCompletion(rcv)
	require.NoError(t, rcv.Shutdown(context.Background()))

	scn := rcv.(*MetricsGenReceiver).scenarios[1]
	for _, inst := range scn.instances {
		assert.Greater(t, inst.id, 1, "instances were replaced")
		assert.Equal(t, staggerOffset(cfg, 1, inst.id), inst.intervalOffset)
	}
}

func TestExplicitIntervalOffsetWinsOverStagger(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.StaggerInstances = true
	cfg.Scenarios[0].InstancesPath = ""
	cfg.Scenarios[0].TemplateVars = map[string]any{"interfaces": []any{"lo"}, "disks": []any{"sda"}, "memory_total": 1024}
	cfg.Scenarios[0].Instances = []InstanceCfg{
		{ResourceAttributes: map[string]any{"host.name": "a"}, IntervalOffset: durationPtr(5 * time.Second)},
		{ResourceAttributes: map[string]any{"host.name": "b"}, IntervalOffset: durationPtr(0)},
		{ResourceAttributes: map[string]any{"host.name": "c"}},
	}
	require.NoError(t, cfg.Validate())
	timestamps := instanceTimestamps(t, runReceiverToCompletion(t, cfg))
	wantOffsets := map[string]time.Duration{"a": 5 * time.Second, "b": 0, "c": staggerOffset(cfg, 0, 2)}
	require.NotZero(t, wantOffsets["c"])
	for host, offset := range wantOffsets {
		require.Len(t, timestamps[host], 3, host)
		for k, ts := range timestamps[host] {
			assert.Equal(t, cfg.StartTime.Add(time.Duration(k)*cfg.Interval+offset), ts, host)
		}
	}

	// Without stagger_instances, unset offsets are 0.
	cfg.StaggerInstances = false
	timestamps = instanceTimestamps(t, runReceiverToCompletion(t, cfg))
	assert.Equal(t, cfg.StartTime.Add(5*time.Second), timestamps["a"][0])
	assert.Equal(t, cfg.StartTime, timestamps["b"][0])
	assert.Equal(t, cfg.StartTime, timestamps["c"][0])
}

func TestExplicitStartTimestamp(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.Scenarios[0].InstancesPath = ""
	cfg.Scenarios[0].TemplateVars = map[string]any{"interfaces": []any{"lo"}, "disks": []any{"sda"}, "memory_total": 1024}
	hostStart := time.Date(2024, 12, 16, 8, 30, 0, 0, time.UTC)
	cfg.Scenarios[0].Instances = []InstanceCfg{
		{ResourceAttributes: map[string]any{"host.name": "a"}, StartTimestamp: timePtr(hostStart)},
		{ResourceAttributes: map[string]any{"host.name": "b"}},
		{ResourceAttributes: map[string]any{"host.name": "c"}, StartTimestamp: timePtr(cfg.StartTime)},
	}
	require.NoError(t, cfg.Validate())
	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 9)
	wantStart := map[string]time.Time{"a": hostStart, "b": cfg.StartTime, "c": cfg.StartTime}
	for _, md := range allMetrics {
		host := resourceHostName(t, md)
		dp.ForEachDataPoint(&md, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
			assert.Equal(t, wantStart[host], dataPoint.StartTimestamp().AsTime(), "%s %s", host, m.Name())
		})
	}
}

func TestInstanceTimingValidate(t *testing.T) {
	startTime := time.Date(2024, 12, 17, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		instance  InstanceCfg
		wantError string
	}{
		{name: "zero offset", instance: InstanceCfg{IntervalOffset: durationPtr(0)}},
		{name: "largest offset", instance: InstanceCfg{IntervalOffset: durationPtr(30*time.Second - 1)}},
		{name: "start timestamp at start time", instance: InstanceCfg{StartTimestamp: timePtr(startTime)}},
		{
			name:      "negative offset",
			instance:  InstanceCfg{IntervalOffset: durationPtr(-time.Second)},
			wantError: "instances[0]: interval_offset (-1s) must not be negative",
		},
		{
			name:      "offset equal to interval",
			instance:  InstanceCfg{IntervalOffset: durationPtr(30 * time.Second)},
			wantError: "instances[0]: interval_offset (30s) must be less than interval (30s)",
		},
		{
			name:      "start timestamp after start time",
			instance:  InstanceCfg{StartTimestamp: timePtr(startTime.Add(time.Second))},
			wantError: "instances[0]: start_timestamp (2024-12-17T00:00:01Z) must not be after the receiver's start_time (2024-12-17T00:00:00Z)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				Interval:  30 * time.Second,
				StartTime: startTime,
				EndTime:   startTime.Add(time.Minute),
				Scenarios: []ScenarioCfg{{Path: instancesTemplatePath, Instances: []InstanceCfg{tc.instance}}},
			}
			err := cfg.Validate()
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantError)
			}
		})
	}
}

func TestInstanceTimingErrorsWhenCreatingReceiver(t *testing.T) {
	t.Run("file offset", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "instances.ndjson")
		require.NoError(t, os.WriteFile(path, []byte(`{"interval_offset":"45s"}`+"\n"), 0o600))
		cfg := explicitInstancesFileConfig()
		cfg.Scenarios[0].InstancesPath = path
		_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
		require.EqualError(t, err, path+":1: interval_offset (45s) must be less than interval (30s)")
	})
	t.Run("file start timestamp", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "instances.ndjson")
		require.NoError(t, os.WriteFile(path, []byte("{}\n"+`{"start_timestamp":"2024-12-17T00:00:01Z"}`+"\n"), 0o600))
		cfg := explicitInstancesFileConfig()
		cfg.Scenarios[0].InstancesPath = path
		_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
		require.EqualError(t, err, path+":2: start_timestamp (2024-12-17T00:00:01Z) must not be after the receiver's start_time (2024-12-17T00:00:00Z)")
	})
	t.Run("start time derived from start_now_minus", func(t *testing.T) {
		cfg := NewFactory().CreateDefaultConfig().(*Config)
		cfg.Interval = 30 * time.Second
		cfg.StartNowMinus = time.Hour
		cfg.Scenarios = []ScenarioCfg{{
			Path:      instancesTemplatePath,
			Instances: []InstanceCfg{{StartTimestamp: timePtr(time.Now().Add(time.Hour))}},
		}}
		require.NoError(t, cfg.Validate())
		_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
		require.ErrorContains(t, err, "instances[0]: start_timestamp")
	})
}

func TestLoadConfigInstanceTiming(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(`metricsgen:
  interval: 30s
  stagger_instances: true
  scenarios:
  - path: testdata/instances-template
    instances:
    - interval_offset: 12.5s
      start_timestamp: "2024-12-16T08:30:00Z"
    - interval_offset: 0s
`), 0o600))
	ndjsonPath := filepath.Join(dir, "instances.ndjson")
	require.NoError(t, os.WriteFile(ndjsonPath, []byte(
		`{"interval_offset":"12.5s","start_timestamp":"2024-12-16T08:30:00Z"}`+"\n"+`{"interval_offset":"0s"}`+"\n"), 0o600))

	cm, err := confmaptest.LoadConf(yamlPath)
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	sub, err := cm.Sub("metricsgen")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))
	assert.True(t, cfg.StaggerInstances)

	want := []InstanceCfg{
		{IntervalOffset: durationPtr(12500 * time.Millisecond), StartTimestamp: timePtr(time.Date(2024, 12, 16, 8, 30, 0, 0, time.UTC))},
		{IntervalOffset: durationPtr(0)},
	}
	assert.Equal(t, want, cfg.Scenarios[0].Instances)
	fromFile, err := loadInstancesFile(ndjsonPath, cfg.Interval)
	require.NoError(t, err)
	assert.Equal(t, want, instanceCfgs(fromFile))
}

// instanceTimestamps returns the data point timestamp of every batch, per host.name. All data
// points of a batch must have the same timestamp.
func instanceTimestamps(t *testing.T, allMetrics []pmetric.Metrics) map[string][]time.Time {
	t.Helper()
	timestamps := map[string][]time.Time{}
	for _, md := range allMetrics {
		host := resourceHostName(t, md)
		timestamps[host] = append(timestamps[host], batchTimestamp(t, md))
	}
	return timestamps
}

func batchTimestamp(t *testing.T, md pmetric.Metrics) time.Time {
	t.Helper()
	var ts pcommon.Timestamp
	dp.ForEachDataPoint(&md, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
		if idx == 0 {
			ts = dataPoint.Timestamp()
		}
		require.Equal(t, ts, dataPoint.Timestamp())
	})
	return ts.AsTime()
}

// marshalWithoutTimestamps serialises the metrics with every data point timestamp set to 0.
func marshalWithoutTimestamps(t *testing.T, allMetrics []pmetric.Metrics) string {
	t.Helper()
	cleared := make([]pmetric.Metrics, len(allMetrics))
	for i, md := range allMetrics {
		clone := pmetric.NewMetrics()
		md.CopyTo(clone)
		dp.ForEachDataPoint(&clone, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
			dataPoint.SetTimestamp(0)
		})
		cleared[i] = clone
	}
	return marshalMetricsOutput(t, cleared).String()
}

func uniqueDurations(m map[string]time.Duration) map[time.Duration]struct{} {
	unique := map[time.Duration]struct{}{}
	for _, d := range m {
		unique[d] = struct{}{}
	}
	return unique
}
