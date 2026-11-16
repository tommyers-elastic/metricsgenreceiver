package metricsgenreceiver

import (
	"testing"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// streamsTemplatePath has a gauge, counters, an explicit-bucket histogram and an exponential
// histogram, and a resource attributes template that only uses random placeholders.
const streamsTemplatePath = "testdata/streams-template"

func TestSplitMix64Source(t *testing.T) {
	src := &splitMix64{}
	// The first outputs of splitmix64 seeded with 0.
	assert.Equal(t, uint64(0xe220a8397b1dcdaf), src.Uint64())
	assert.Equal(t, uint64(0x6e789e6aa1b965f4), src.Uint64())

	r := newSplitMix64Rand(42)
	first := []float64{r.NormFloat64(), r.Float64(), float64(r.Int63())}
	buf := make([]byte, 3)
	_, _ = r.Read(buf)
	reseed(r, 42)
	assert.Equal(t, first, []float64{r.NormFloat64(), r.Float64(), float64(r.Int63())})
	// Reseeding discards bytes buffered by Read.
	reseed(r, 7)
	want := make([]byte, 8)
	_, _ = r.Read(want)
	_, _ = r.Read(buf)
	reseed(r, 7)
	got := make([]byte, 8)
	_, _ = r.Read(got)
	assert.Equal(t, want, got)
}

// streamsInstance is an explicit instance of the streams template.
func streamsInstance(host string, interfaces []any, modify func(*InstanceCfg)) InstanceCfg {
	inst := InstanceCfg{
		ResourceAttributes: map[string]any{"host.name": host},
		TemplateVars:       map[string]any{"interfaces": interfaces},
	}
	if modify != nil {
		modify(&inst)
	}
	return inst
}

// streamsInstances are six instances in three template groups, with every timing option.
func streamsInstances() []InstanceCfg {
	two := []any{"lo", "eth0"}
	three := []any{"lo", "eth0", "eth1"}
	return []InstanceCfg{
		streamsInstance("a", two, nil),
		streamsInstance("b", two, func(c *InstanceCfg) {
			c.StartTimestamp = at(-time.Hour)
			c.Restarts = []time.Time{*at(125 * time.Second)}
		}),
		streamsInstance("c", three, func(c *InstanceCfg) { c.EndTime = at(3 * time.Minute) }),
		streamsInstance("d", three, func(c *InstanceCfg) { c.IntervalOffset = durationPtr(7 * time.Second) }),
		streamsInstance("e", []any{"lo"}, nil),
		streamsInstance("f", two, func(c *InstanceCfg) { c.StartTime = at(45 * time.Second) }),
	}
}

func streamsConfig(scenarios ...ScenarioCfg) *Config {
	cfg := lifecycleConfig()
	cfg.StaggerInstances = true
	cfg.IntervalJitterStdDev = 5 * time.Millisecond
	cfg.Scenarios = scenarios
	return cfg
}

// TestExplicitInstancesSubsetOutput checks that a run over a subset of the instances, in another
// order and with another scenario before them, emits exactly the same data for every instance of
// the subset as the run over all instances: values, timestamps and resource attributes. With
// temporality_override cumulative, the histograms are cumulative too.
func TestExplicitInstancesSubsetOutput(t *testing.T) {
	for _, temporality := range []string{"", "cumulative"} {
		t.Run("temporality "+temporality, func(t *testing.T) {
			testExplicitInstancesSubsetOutput(t, temporality)
		})
	}
}

func testExplicitInstancesSubsetOutput(t *testing.T, temporality string) {
	all := streamsInstances()
	fullCfg := streamsConfig(ScenarioCfg{Path: streamsTemplatePath, Instances: all, TemporalityOverride: temporality})
	require.NoError(t, fullCfg.Validate())
	subsetCfg := streamsConfig(
		ScenarioCfg{Path: streamsTemplatePath, Scale: 2, TemplateVars: map[string]any{"interfaces": []any{"eth0"}}},
		ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{all[4], all[1], all[3], all[5]}, TemporalityOverride: temporality},
	)
	require.NoError(t, subsetCfg.Validate())

	full := batchesByHost(t, runReceiverToCompletion(t, fullCfg))
	subset := batchesByHost(t, runReceiverToCompletion(t, subsetCfg))
	require.Len(t, full, 6)

	for _, host := range []string{"b", "d", "e", "f"} {
		require.NotEmpty(t, full[host], host)
		assert.Equal(t, marshalMetricsOutput(t, full[host]).String(), marshalMetricsOutput(t, subset[host]).String(), host)
	}
	// The generated instances of the other scenario have an empty host name.
	assert.Len(t, subset, 4+1)
	assert.Len(t, subset[""], 2*10)

	// Determinism, and the instances do differ from each other.
	again := batchesByHost(t, runReceiverToCompletion(t, fullCfg))
	outputs := map[string]struct{}{}
	for host, batches := range full {
		output := marshalMetricsOutput(t, batches).String()
		assert.Equal(t, output, marshalMetricsOutput(t, again[host]).String(), host)
		outputs[marshalValues(t, batches[len(batches)-1])] = struct{}{}
	}
	assert.Len(t, outputs, 6)

	// The exponential histograms have the temporality under test.
	wantTemporality := pmetric.AggregationTemporalityDelta
	if temporality == "cumulative" {
		wantTemporality = pmetric.AggregationTemporalityCumulative
	}
	exponentialHistograms := 0
	for _, batches := range full {
		md := batches[len(batches)-1]
		dp.ForEachMetric(&md, func(_ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric) {
			if m.Type() == pmetric.MetricTypeExponentialHistogram {
				exponentialHistograms++
				assert.Equal(t, wantTemporality, m.ExponentialHistogram().AggregationTemporality())
			}
		})
	}
	assert.Equal(t, 6, exponentialHistograms)
}

// TestExplicitInstancesDoNotDrawFromSharedRand checks that a scenario of generated instances
// emits the same data whether or not a scenario of explicit instances comes before it.
func TestExplicitInstancesDoNotDrawFromSharedRand(t *testing.T) {
	generated := ScenarioCfg{Path: streamsTemplatePath, Scale: 3, TemplateVars: map[string]any{"interfaces": []any{"eth0"}}}
	alone := streamsConfig(generated)
	alone.StaggerInstances = false
	mixed := streamsConfig(ScenarioCfg{Path: streamsTemplatePath, Instances: streamsInstances()}, generated)
	mixed.StaggerInstances = false

	want := runReceiverToCompletion(t, alone)
	var got []pmetric.Metrics
	explicitHosts := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true, "f": true}
	for _, md := range runReceiverToCompletion(t, mixed) {
		if !explicitHosts[resourceHostName(t, md)] {
			got = append(got, md)
		}
	}
	require.Len(t, want, 30)
	assert.Equal(t, marshalMetricsOutput(t, want).String(), marshalMetricsOutput(t, got).String())
}

// TestExplicitInstancesDoNotDrawFromSharedRandStaggered checks that with stagger_instances, a
// scenario of generated instances emits the same values whether or not a scenario of explicit
// instances is configured, and the same timestamps too when it keeps its position.
func TestExplicitInstancesDoNotDrawFromSharedRandStaggered(t *testing.T) {
	generated := ScenarioCfg{Path: streamsTemplatePath, Scale: 3, TemplateVars: map[string]any{"interfaces": []any{"eth0"}}}
	explicit := ScenarioCfg{Path: streamsTemplatePath, Instances: streamsInstances()}
	explicitHosts := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true, "f": true}
	generatedOnly := func(cfg *Config) []pmetric.Metrics {
		var out []pmetric.Metrics
		for _, md := range runReceiverToCompletion(t, cfg) {
			if !explicitHosts[resourceHostName(t, md)] {
				out = append(out, md)
			}
		}
		return out
	}

	want := runReceiverToCompletion(t, streamsConfig(generated))
	require.Len(t, want, 30)
	// The generated scenario moves from position 0 to 1: the same values at other offsets.
	moved := generatedOnly(streamsConfig(explicit, generated))
	assert.Equal(t, marshalWithoutTimestamps(t, want), marshalWithoutTimestamps(t, moved))
	assert.NotEqual(t, marshalMetricsOutput(t, want).String(), marshalMetricsOutput(t, moved).String())
	// It keeps its position: identical output.
	kept := generatedOnly(streamsConfig(generated, explicit))
	assert.Equal(t, marshalMetricsOutput(t, want).String(), marshalMetricsOutput(t, kept).String())
}

// TestExplicitInstanceInTwoScenarios checks that the same definition declared in two scenarios
// with the same path emits identical data in both.
func TestExplicitInstanceInTwoScenarios(t *testing.T) {
	instance := streamsInstance("b", []any{"lo", "eth0"}, func(c *InstanceCfg) {
		c.Restarts = []time.Time{*at(125 * time.Second)}
	})
	cfg := streamsConfig(
		ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{instance}},
		ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{streamsInstance("a", []any{"lo"}, nil), instance}},
	)
	require.NoError(t, cfg.Validate())
	batches := batchesByHost(t, runReceiverToCompletion(t, cfg))["b"]
	require.Len(t, batches, 20)
	// For every tick, the scenarios are emitted in order.
	for i := 0; i < len(batches); i += 2 {
		assert.Equal(t, marshalMetricsOutput(t, batches[i:i+1]).String(), marshalMetricsOutput(t, batches[i+1:i+2]).String(), "tick %d", i/2)
	}
}

// TestExplicitInstanceTemplateVarsEqualInJSON checks that template variables that differ only in
// their type (1 and 1.0) form separate templates that advance with the same random stream.
func TestExplicitInstanceTemplateVarsEqualInJSON(t *testing.T) {
	cfg := streamsConfig(ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{
		{ResourceAttributes: map[string]any{"host.name": "a"}, TemplateVars: map[string]any{"interfaces": []any{"lo"}, "x": 1}},
		{ResourceAttributes: map[string]any{"host.name": "a"}, TemplateVars: map[string]any{"interfaces": []any{"lo"}, "x": 1.0}},
	}})
	require.NoError(t, cfg.Validate())
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	require.Len(t, rcv.scenarios, 2, "separate templates")
	first, second := rcv.scenarios[0], rcv.scenarios[1]
	assert.Equal(t, first.rand.Int63(), second.rand.Int63(), "the same random stream")
	// They are repeats of one definition, so the instances still differ.
	assert.NotEqual(t, first.instances[0].key, second.instances[0].key)
}

func TestExplicitInstanceKeys(t *testing.T) {
	base := streamsConfig(ScenarioCfg{Path: streamsTemplatePath, Instances: streamsInstances()})
	keys := explicitInstanceKeys(t, base)
	require.Len(t, keys, 6)
	unique := map[uint64]struct{}{}
	for _, key := range keys {
		unique[key] = struct{}{}
	}
	assert.Len(t, unique, 6)

	// Timing fields, list position, other instances and other scenarios do not change a key.
	moved := streamsConfig(
		ScenarioCfg{Path: streamsTemplatePath, Scale: 1, TemplateVars: map[string]any{"interfaces": []any{"lo"}}},
		ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{
			streamsInstance("e", []any{"lo"}, func(c *InstanceCfg) { c.EndTime = at(time.Minute) }),
			streamsInstance("a", []any{"lo", "eth0"}, func(c *InstanceCfg) { c.IntervalOffset = durationPtr(time.Second) }),
		}},
	)
	moved.InstanceOffset = 5
	movedKeys := explicitInstanceKeys(t, moved)
	assert.Equal(t, keys["a"], movedKeys["a"])
	assert.Equal(t, keys["e"], movedKeys["e"])

	// The seed, the scenario path, the template variables and the resource attributes do.
	changed := func(modify func(cfg *Config)) uint64 {
		cfg := streamsConfig(ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{streamsInstance("a", []any{"lo", "eth0"}, nil)}})
		modify(cfg)
		return explicitInstanceKeys(t, cfg)["a"]
	}
	assert.Equal(t, keys["a"], changed(func(*Config) {}))
	assert.NotEqual(t, keys["a"], changed(func(cfg *Config) { cfg.Seed++ }))
	assert.NotEqual(t, keys["a"], changed(func(cfg *Config) { cfg.Scenarios[0].Path = lifecycleTemplatePath }))
	assert.NotEqual(t, keys["a"], changed(func(cfg *Config) { cfg.Scenarios[0].TemplateVars = map[string]any{"x": 1} }))
	assert.NotEqual(t, keys["a"], changed(func(cfg *Config) { cfg.Scenarios[0].Instances[0].ResourceAttributes["x"] = 1 }))
	assert.NotEqual(t, changed(func(cfg *Config) { cfg.Scenarios[0].Instances[0].ResourceAttributes["x"] = 1 }),
		changed(func(cfg *Config) { cfg.Scenarios[0].Instances[0].ResourceAttributes["x"] = 1.0 }))
}

func TestExplicitInstanceKeyIsPinned(t *testing.T) {
	// Integer arithmetic and hashing only: the same on every architecture.
	attrs := pcommon.NewMap()
	attrs.PutStr("host.name", "web-1")
	attrs.PutInt("host.cpu.count", 4)
	groupHash := templateVarsHash(explicitScenarioHash(7, "my-hosts"), 0x1234)
	key := explicitInstanceKey(groupHash, attributesDigest(attrs), 0)
	assert.Equal(t, uint64(0x6aa482d5b2880518), key)
	assert.NotEqual(t, key, explicitInstanceKey(groupHash, attributesDigest(attrs), 1))

	cfg := &Config{Interval: 30 * time.Second, StaggerInstances: true}
	offset := staggerOffsetForKey(cfg, key)
	assert.Equal(t, time.Duration(20375158648), offset)
	assert.GreaterOrEqual(t, offset, time.Duration(0))
	assert.Less(t, offset, cfg.Interval)
	cfg.StaggerInstances = false
	assert.Equal(t, time.Duration(0), staggerOffsetForKey(cfg, key))
}

func TestExplicitInstanceDuplicatesDiffer(t *testing.T) {
	duplicate := streamsInstance("a", []any{"lo", "eth0"}, nil)
	cfg := streamsConfig(ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{
		duplicate,
		streamsInstance("b", []any{"lo", "eth0"}, nil),
		duplicate,
		duplicate,
	}})
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	require.Len(t, rcv.scenarios, 1)
	instances := rcv.scenarios[0].instances
	require.Len(t, instances, 4)
	assert.NotEqual(t, instances[0].key, instances[2].key)
	assert.NotEqual(t, instances[0].key, instances[3].key)
	assert.NotEqual(t, instances[2].key, instances[3].key)

	// Each repeat emits other values and other resource attributes from the template.
	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 40)
	values := map[string]struct{}{}
	hostIDs := map[string]struct{}{}
	for _, md := range allMetrics[36:] {
		values[marshalValues(t, md)] = struct{}{}
		hostID, ok := md.ResourceMetrics().At(0).Resource().Attributes().Get("host.id")
		require.True(t, ok)
		hostIDs[hostID.Str()] = struct{}{}
	}
	assert.Len(t, values, 4)
	assert.Len(t, hostIDs, 4)

	// The first definition keeps its key when a repeat is added after it.
	firstOnly := streamsConfig(ScenarioCfg{Path: streamsTemplatePath, Instances: []InstanceCfg{duplicate}})
	assert.Equal(t, instances[0].key, explicitInstanceKeys(t, firstOnly)["a"])
}

func TestExplicitInstanceIDIsListPosition(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.InstanceOffset = 3
	cfg.Scenarios = []ScenarioCfg{{
		Path: "testdata/histogram-template",
		Instances: []InstanceCfg{
			{ResourceAttributes: map[string]any{"host.name": "a"}},
			{ResourceAttributes: map[string]any{"host.name": "b"}},
		},
	}}
	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 6)
	for i, md := range allMetrics {
		id, ok := md.ResourceMetrics().At(0).Resource().Attributes().Get("service.instance.id")
		require.True(t, ok)
		assert.Equal(t, []string{"instance-3", "instance-4"}[i%2], id.Str())
	}
}

// explicitInstanceKeys returns the key of every explicit instance of cfg, by host.name.
func explicitInstanceKeys(t *testing.T, cfg *Config) map[string]uint64 {
	t.Helper()
	require.NoError(t, cfg.Validate())
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	keys := map[string]uint64{}
	for _, scn := range rcv.scenarios {
		if !scn.explicitInstances {
			continue
		}
		for _, inst := range scn.instances {
			host, ok := inst.explicitAttributes.Get("host.name")
			require.True(t, ok)
			require.NotContains(t, keys, host.Str())
			keys[host.Str()] = inst.key
			assert.Equal(t, variationID(inst.key), inst.variationID)
			assert.GreaterOrEqual(t, inst.variationID, 0)
		}
	}
	return keys
}

// marshalValues serialises md without its resource attributes and timestamps.
func marshalValues(t *testing.T, md pmetric.Metrics) string {
	t.Helper()
	clone := pmetric.NewMetrics()
	md.CopyTo(clone)
	for i := 0; i < clone.ResourceMetrics().Len(); i++ {
		clone.ResourceMetrics().At(i).Resource().Attributes().Clear()
	}
	dp.ForEachDataPoint(&clone, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
		dataPoint.SetTimestamp(0)
		dataPoint.SetStartTimestamp(0)
	})
	return marshalMetricsOutput(t, []pmetric.Metrics{clone}).String()
}
