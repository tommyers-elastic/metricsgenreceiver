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

const (
	// scopesTemplatePath has three scopes: CPU time and utilization by state (share_counter and
	// share_gauge), memory usage by state (share_gauge), and a load average (bounded_gauge) next
	// to an unhinted counter.
	scopesTemplatePath = "testdata/scopes-template"
	scopesTestStagger  = 50 * time.Millisecond
)

var scopeNames = []string{"metricsgen.scopes.test/cpu", "metricsgen.scopes.test/memory", "metricsgen.scopes.test/processes"}

// scopesConfig runs a scenario of three generated instances and one of three explicit instances
// of the scopes template, with stagger_instances, jitter and the given stagger_scopes.
func scopesConfig(staggerScopes time.Duration, explicit ...InstanceCfg) *Config {
	cfg := modelTestConfig(scopesTemplatePath, 60, func(scn *ScenarioCfg) { scn.Scale = 3 })
	cfg.StaggerInstances = true
	cfg.IntervalJitterStdDev = 5 * time.Millisecond
	cfg.StaggerScopes = staggerScopes
	if explicit == nil {
		explicit = scopesInstances()
	}
	cfg.Scenarios = append(cfg.Scenarios, ScenarioCfg{Path: scopesTemplatePath, Instances: explicit})
	return cfg
}

// scopesInstances are explicit instances: one plain, one that restarts between ticks and one
// that starts exactly at a sample time.
func scopesInstances() []InstanceCfg {
	return []InstanceCfg{
		{ResourceAttributes: map[string]any{"host.name": "a"}},
		{ResourceAttributes: map[string]any{"host.name": "b"}, Restarts: []time.Time{modelTestStart.Add(95 * time.Second)}},
		{ResourceAttributes: map[string]any{"host.name": "c"}, IntervalOffset: durationPtr(0), StartTime: timePtr(modelTestStart.Add(30 * time.Second))},
	}
}

// scopeTimestamps returns the timestamp of each scope of md, by scope name. All data points of a
// scope must have the same timestamp.
func scopeTimestamps(t *testing.T, md pmetric.Metrics) map[string]time.Time {
	t.Helper()
	timestamps := map[string]time.Time{}
	dp.ForEachDataPoint(&md, func(_ int, _ pcommon.Resource, is pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
		ts := dataPoint.Timestamp().AsTime()
		if existing, ok := timestamps[is.Name()]; ok {
			require.Equal(t, existing, ts, is.Name())
		}
		timestamps[is.Name()] = ts
	})
	return timestamps
}

func TestStaggerScopesOffByDefault(t *testing.T) {
	assert.Zero(t, NewFactory().CreateDefaultConfig().(*Config).StaggerScopes)
	cfg := scopesConfig(0)
	require.NoError(t, cfg.Validate())
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	for _, scn := range rcv.scenarios {
		assert.Nil(t, scn.seriesScopeHashes)
	}
	for _, md := range runReceiverToCompletion(t, cfg) {
		// All data points of a batch have the same timestamp.
		batchTimestamp(t, md)
	}
}

func TestStaggerScopes(t *testing.T) {
	cfg := scopesConfig(scopesTestStagger)
	require.NoError(t, cfg.Validate())
	staggered := runReceiverToCompletion(t, cfg)
	unstaggered := runReceiverToCompletion(t, scopesConfig(0))
	// The lifetimes are evaluated at the sample time, before the scope offsets: the same samples.
	require.Len(t, staggered, len(unstaggered))
	require.Len(t, staggered, 60*3+60+60+57)

	offsets := map[string]map[string]time.Duration{}
	for i, md := range staggered {
		host := resourceHostName(t, md)
		require.Equal(t, host, resourceHostName(t, unstaggered[i]), "batch %d", i)
		instanceTimestamp := batchTimestamp(t, unstaggered[i])
		timestamps := scopeTimestamps(t, md)
		require.Len(t, timestamps, 3)
		if offsets[host] == nil {
			offsets[host] = map[string]time.Duration{}
		}
		for scope, ts := range timestamps {
			offset := ts.Sub(instanceTimestamp)
			assert.GreaterOrEqual(t, offset, time.Duration(0), "batch %d %s", i, scope)
			assert.Less(t, offset, scopesTestStagger, "batch %d %s", i, scope)
			// Fixed per instance and scope.
			if want, ok := offsets[host][scope]; ok {
				require.Equal(t, want, offset, "batch %d %s", i, scope)
			}
			offsets[host][scope] = offset
		}
		// The values that do not depend on the timestamps are the same.
		assert.Equal(t, metricValue(t, unstaggered[i], "system.processes.created"), metricValue(t, md, "system.processes.created"), "batch %d", i)
	}
	require.Len(t, offsets, 6)
	for host, byScope := range offsets {
		assert.Len(t, uniqueDurations(byScope), 3, "offsets differ between the scopes of %s", host)
	}
	for _, scope := range scopeNames {
		byHost := map[string]time.Duration{}
		for host, byScope := range offsets {
			byHost[host] = byScope[scope]
		}
		assert.Len(t, uniqueDurations(byHost), 6, "offsets of %s differ between instances", scope)
	}

	// The offsets are scopeOffset of the instance and the scope.
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	checked := 0
	for k := range rcv.scenarios {
		scn := &rcv.scenarios[k]
		require.NotNil(t, scn.seriesScopeHashes)
		for i := range scn.instances {
			instance := &scn.instances[i]
			host, ok := instance.resource.Attributes().Get("host.name")
			if scn.explicitInstances {
				host, ok = instance.explicitAttributes.Get("host.name")
			}
			require.True(t, ok)
			for _, scope := range scopeNames {
				is := pcommon.NewInstrumentationScope()
				is.SetName(scope)
				is.SetVersion("1.0.0")
				want := scopeOffset(instanceScopeHash(cfg, scn, instance), scopeHash(is), cfg.StaggerScopes)
				assert.Equal(t, want, offsets[host.Str()][scope], "%s %s", host.Str(), scope)
				checked++
			}
		}
	}
	assert.Equal(t, 6*3, checked)
}

// TestStaggerScopesModels checks that the stateless value models are evaluated at the scopes'
// timestamps and that share groups still add up.
func TestStaggerScopesModels(t *testing.T) {
	cfg := scopesConfig(scopesTestStagger)
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	loadIndex := -1
	dp.ForEachDataPoint(rcv.scenarios[0].metricsTemplate, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, _ dp.DataPoint) {
		if m.Name() == "system.cpu.load_average.1m" {
			loadIndex = idx
		}
	})
	require.GreaterOrEqual(t, loadIndex, 0)
	variationIDs := map[string]int{}
	for _, scn := range rcv.scenarios {
		for _, instance := range scn.instances {
			host, _ := instance.resource.Attributes().Get("host.name")
			if scn.explicitInstances {
				host, _ = instance.explicitAttributes.Get("host.name")
			}
			variationIDs[host.Str()] = instance.variationID
		}
	}

	byHost := batchesByHost(t, runReceiverToCompletion(t, cfg))
	require.Len(t, byHost, 6)
	for host, batches := range byHost {
		for i, md := range batches {
			timestamps := scopeTimestamps(t, md)
			start := batchStartTimestamp(t, md)
			groups := shareGroupValues(t, md, "state")
			// CPU time adds up to the total rate (1 s/s) times the time since the start, at the
			// CPU scope's timestamp; with a 50ms stagger, the instance's timestamp would be off by
			// more than the rounding of three members to 2 decimals.
			elapsed := timestamps["metricsgen.scopes.test/cpu"].Sub(start).Seconds()
			require.InDelta(t, elapsed, sumValues(groups["system.cpu.time|"]), 3*0.005+1e-6, "%s sample %d", host, i)
			require.InDelta(t, 1, sumValues(groups["system.cpu.utilization|"]), 0.01+1e-9, "%s sample %d", host, i)
			require.Equal(t, 1e10, sumValues(groups["system.memory.usage|"]), "%s sample %d", host, i)

			// The load average is the model's value at the timestamp of its scope.
			want := pmetric.NewNumberDataPoint()
			want.SetDoubleValue(1.5)
			want.SetTimestamp(pcommon.NewTimestampFromTime(timestamps["metricsgen.scopes.test/processes"]))
			rcv.scenarios[0].models.Apply(want, loadIndex, variationIDs[host], nil)
			require.Equal(t, want.DoubleValue(), metricValue(t, md, "system.cpu.load_average.1m"), "%s sample %d", host, i)
		}
	}
}

// TestStaggerScopesExplicitInstancesSubset checks that with stagger_scopes, a run over a subset
// of the explicit instances in another order still emits the same data for them.
func TestStaggerScopesExplicitInstancesSubset(t *testing.T) {
	all := scopesInstances()
	full := marshalByHost(t, runReceiverToCompletion(t, scopesConfig(scopesTestStagger, all...)))
	subsetCfg := scopesConfig(scopesTestStagger, all[2], all[1])
	subsetCfg.Scenarios[0].Scale = 1
	subset := marshalByHost(t, runReceiverToCompletion(t, subsetCfg))
	require.Len(t, subset, 3)
	for _, host := range []string{"b", "c"} {
		require.Contains(t, full, host)
		assert.Equal(t, full[host], subset[host], host)
	}
}

func TestScopeOffsetIsPinned(t *testing.T) {
	// Integer arithmetic and hashing only: the same on every architecture.
	cfg := &Config{Seed: 7, Interval: 30 * time.Second, StaggerScopes: 20 * time.Millisecond}
	is := pcommon.NewInstrumentationScope()
	is.SetName("metricsgen.scopes.test/cpu")
	is.SetVersion("1.0.0")
	generated := &Scenario{configIndex: 1}
	explicit := &Scenario{explicitInstances: true}
	instance := &scenarioInstance{id: 3, key: 0x6aa482d5b2880518}
	assert.Equal(t, time.Duration(15069691), scopeOffset(instanceScopeHash(cfg, generated, instance), scopeHash(is), cfg.StaggerScopes))
	assert.Equal(t, time.Duration(14268319), scopeOffset(instanceScopeHash(cfg, explicit, instance), scopeHash(is), cfg.StaggerScopes))

	// The version is part of the scope's identity.
	other := pcommon.NewInstrumentationScope()
	other.SetName("metricsgen.scopes.test/cpu")
	other.SetVersion("1.0.1")
	assert.NotEqual(t, scopeHash(is), scopeHash(other))
}
