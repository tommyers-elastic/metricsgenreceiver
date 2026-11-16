package metricsgenreceiver

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	// hostTemplatePath is a small synthetic host that uses the realism hints together: CPU time
	// and utilization by state (share_counter and share_gauge with one variation_key), memory
	// usage and utilization by state (share_gauge with one variation_key), filesystem usage by
	// state for two mountpoints (share_gauge), three load averages (bounded_gauge) and network
	// traffic (seeded_rate_counter), in five scopes.
	hostTemplatePath  = "testdata/host-template"
	hostIntervals     = 360
	hostStaggerScopes = 20 * time.Millisecond
	// hostCPUs is the number of CPUs of the template; each adds up to one second per second.
	hostCPUs = 2
	// hostCPUTimeTolerance covers the rounding of the six CPU time members to 2 decimals.
	hostCPUTimeTolerance = 6*0.005 + 1e-6
	// hostMemorySize is what the memory usage by state adds up to.
	hostMemorySize = 8589934592.0
	// hostMemoryUtilizationTolerance covers the rounding of the memory utilization to 5 decimals
	// with the rounding residual of the group moved to its largest member.
	hostMemoryUtilizationTolerance = 2e-5
	// hostCPUShareTolerance bounds the difference between the CPU time's rate share over an
	// interval and the mean of the CPU utilization at its ends: the share varies little over a
	// 10s interval, and the CPU time is rounded to 2 decimals.
	hostCPUShareTolerance = 0.004
	// hostLoadRounding is the rounding of the load averages to 2 decimals.
	hostLoadRounding = 0.005
	// hostInstanceSpread is the default instance_spread of the load averages.
	hostInstanceSpread = 2.0
)

var (
	hostCPUKeys       = []string{"cpu=cpu0", "cpu=cpu1"}
	hostCounterGroups = []string{
		"system.cpu.time|cpu=cpu0",
		"system.cpu.time|cpu=cpu1",
		"system.network.io|device=eth0,direction=receive",
		"system.network.io|device=eth0,direction=transmit",
	}
	// hostLoadAverages are the load averages of the template with their seeds and variations, from
	// the shortest to the longest period.
	hostLoadAverages = []struct {
		name      string
		seed      float64
		variation float64
	}{
		{name: "system.cpu.load_average.1m", seed: 0.62, variation: 0.5},
		{name: "system.cpu.load_average.5m", seed: 0.55, variation: 0.3},
		{name: "system.cpu.load_average.15m", seed: 0.48, variation: 0.2},
	}
)

// hostInstances are a host up since before the window, one started a day before the window and
// one that restarts between two ticks.
func hostInstances() map[string]InstanceCfg {
	return map[string]InstanceCfg{
		"web-1": {ResourceAttributes: map[string]any{"host.name": "web-1"}},
		"web-2": {
			ResourceAttributes: map[string]any{"host.name": "web-2"},
			StartTimestamp:     timePtr(modelTestStart.Add(-24 * time.Hour)),
		},
		"db-1": {
			ResourceAttributes: map[string]any{"host.name": "db-1"},
			Restarts:           []time.Time{modelTestStart.Add(20*time.Minute + 3500*time.Millisecond)},
		},
	}
}

func hostConfig(instances ...InstanceCfg) *Config {
	cfg := modelTestConfig(hostTemplatePath, hostIntervals, func(scn *ScenarioCfg) { scn.Instances = instances })
	cfg.StaggerInstances = true
	cfg.StaggerScopes = hostStaggerScopes
	cfg.IntervalJitterStdDev = 5 * time.Millisecond
	return cfg
}

// TestHostTemplateRealism runs explicit instances of the host template with stagger_instances,
// stagger_scopes, jitter and a restart, and checks the realism properties together.
func TestHostTemplateRealism(t *testing.T) {
	instances := hostInstances()
	cfg := hostConfig(instances["web-1"], instances["web-2"], instances["db-1"])
	require.NoError(t, cfg.Validate())
	all := runReceiverToCompletion(t, cfg)
	requireNoHintMetadata(t, all)
	byHost := batchesByHost(t, all)
	require.Len(t, byHost, 3)
	for host, batches := range byHost {
		require.Len(t, batches, hostIntervals, host)
		assertHostBatches(t, host, batches)
	}

	// The restart is observed.
	restart := instances["db-1"].Restarts[0]
	starts := map[time.Time]int{}
	for _, batch := range byHost["db-1"] {
		starts[batchStartTimestamp(t, batch)]++
	}
	require.Len(t, starts, 2)
	assert.Contains(t, starts, modelTestStart)
	assert.Contains(t, starts, restart)

	// Deterministic.
	assert.Equal(t, marshalMetricsOutput(t, all).String(), marshalMetricsOutput(t, runReceiverToCompletion(t, cfg)).String())

	// A run over a subset of the instances, in another order, emits the same for them.
	subset := marshalByHost(t, runReceiverToCompletion(t, hostConfig(instances["db-1"], instances["web-1"])))
	full := marshalByHost(t, all)
	require.Len(t, subset, 2)
	for host, output := range subset {
		assert.Equal(t, full[host], output, host)
	}
}

// assertHostBatches checks the realism properties of the host template for the batches of one
// host, using the data points' own timestamps and start timestamps.
func assertHostBatches(t *testing.T, host string, batches []pmetric.Metrics) {
	t.Helper()
	var previous map[string]map[string]float64
	var previousStart, previousCPUTimestamp time.Time
	var scopeSpacing map[string]time.Duration
	loads := map[string]*seriesRange{}
	loadMoves := map[string]float64{}
	for i, batch := range batches {
		// Each scope at its own offset, fixed relative to the others.
		timestamps := scopeTimestamps(t, batch)
		require.Len(t, timestamps, 5)
		earliest := timestamps["metricsgen.host.test/cpu"]
		for _, ts := range timestamps {
			if ts.Before(earliest) {
				earliest = ts
			}
		}
		spacing := map[string]time.Duration{}
		for scope, ts := range timestamps {
			spacing[scope] = ts.Sub(earliest)
			require.Less(t, spacing[scope], hostStaggerScopes, "%s sample %d %s", host, i, scope)
		}
		require.Len(t, uniqueDurations(spacing), 5, "%s sample %d", host, i)
		if scopeSpacing == nil {
			scopeSpacing = spacing
		}
		require.Equal(t, scopeSpacing, spacing, "%s sample %d", host, i)

		start := batchStartTimestamp(t, batch)
		cpuTimestamp := timestamps["metricsgen.host.test/cpu"]
		groups := shareGroupValues(t, batch, "state")

		// Groups add up: exactly for ints, within one unit of the last decimal for doubles.
		require.Equal(t, hostMemorySize, sumValues(groups["system.memory.usage|"]), "%s sample %d", host, i)
		require.InDelta(t, 1, sumValues(groups["system.memory.utilization|"]), 1e-5+1e-9, "%s sample %d", host, i)
		// The memory utilization by state is the usage by state divided by the memory size: the
		// two metrics share a variation_key.
		require.Len(t, groups["system.memory.utilization|"], len(groups["system.memory.usage|"]), "%s sample %d", host, i)
		for state, usage := range groups["system.memory.usage|"] {
			require.InDelta(t, usage/hostMemorySize, groups["system.memory.utilization|"][state], hostMemoryUtilizationTolerance, "%s sample %d %s", host, i, state)
		}
		require.Equal(t, 107374182400.0, sumValues(groups["system.filesystem.usage|mountpoint=/"]), "%s sample %d", host, i)
		require.Equal(t, 536870912000.0, sumValues(groups["system.filesystem.usage|mountpoint=/data"]), "%s sample %d", host, i)
		cpuTime := 0.0
		for _, cpu := range hostCPUKeys {
			require.InDelta(t, 1, sumValues(groups["system.cpu.utilization|"+cpu]), 0.001+1e-9, "%s sample %d %s", host, i, cpu)
			cpuTime += sumValues(groups["system.cpu.time|"+cpu])
		}
		// CPU time over all CPUs and states is the number of CPUs times the time since the
		// start, also on the first sample after the restart.
		require.InDelta(t, hostCPUs*cpuTimestamp.Sub(start).Seconds(), cpuTime, hostCPUTimeTolerance, "%s sample %d", host, i)

		// Bounds.
		for key, values := range groups {
			for member, value := range values {
				require.GreaterOrEqual(t, value, 0.0, "%s sample %d %s %s", host, i, key, member)
				if strings.Contains(key, ".utilization|") {
					require.LessOrEqual(t, value, 1.0, "%s sample %d %s %s", host, i, key, member)
				}
			}
		}
		// The load averages stay within variation of a centre in [seed/instance_spread,
		// seed*instance_spread], up to their rounding to 2 decimals; see also the check of one
		// centre per series below.
		for _, load := range hostLoadAverages {
			value := groups[load.name+"|"][""]
			require.GreaterOrEqual(t, value, load.seed/hostInstanceSpread*(1-load.variation)-hostLoadRounding, "%s sample %d %s", host, i, load.name)
			require.LessOrEqual(t, value, load.seed*hostInstanceSpread*(1+load.variation)+hostLoadRounding, "%s sample %d %s", host, i, load.name)
			if loads[load.name] == nil {
				loads[load.name] = &seriesRange{}
			}
			loads[load.name].add(value)
			if previous != nil {
				loadMoves[load.name] += math.Abs(value - previous[load.name+"|"][""])
			}
		}

		if previous != nil && start.Equal(previousStart) {
			// Counters are monotonic for one start timestamp.
			for _, key := range hostCounterGroups {
				for member, value := range groups[key] {
					require.GreaterOrEqual(t, value, previous[key][member], "%s sample %d %s %s", host, i, key, member)
				}
			}
			// The CPU utilization by state is the CPU time's rate share (one second per second
			// per CPU): over an interval, about the mean of the utilization at its ends.
			elapsed := cpuTimestamp.Sub(previousCPUTimestamp).Seconds()
			for _, cpu := range hostCPUKeys {
				for state, value := range groups["system.cpu.time|"+cpu] {
					rateShare := (value - previous["system.cpu.time|"+cpu][state]) / elapsed
					utilization := (groups["system.cpu.utilization|"+cpu][state] + previous["system.cpu.utilization|"+cpu][state]) / 2
					require.InDelta(t, utilization, rateShare, hostCPUShareTolerance, "%s sample %d %s %s", host, i, cpu, state)
				}
			}
		}
		previous, previousStart, previousCPUTimestamp = groups, start, cpuTimestamp
	}
	// All values of a load average lie within centre * (1 ± variation) of one centre in
	// [seed/instance_spread, seed*instance_spread], up to the rounding: some centre is at least
	// max/(1+variation) and at most min/(1-variation).
	for _, load := range hostLoadAverages {
		r := loads[load.name]
		lowest := math.Max((r.max-hostLoadRounding)/(1+load.variation), load.seed/hostInstanceSpread)
		highest := math.Min((r.min+hostLoadRounding)/(1-load.variation), load.seed*hostInstanceSpread)
		assert.LessOrEqual(t, lowest, highest, "%s %s: no centre for values in [%v, %v]", host, load.name, r.min, r.max)
		assert.Greater(t, r.distinct(), 5, "%s %s", host, load.name)
	}
	// The shorter the period, the more a load average moves from sample to sample.
	assert.Greater(t, loadMoves["system.cpu.load_average.1m"], loadMoves["system.cpu.load_average.15m"], host)
}
