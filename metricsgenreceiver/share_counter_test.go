package metricsgenreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const (
	shareCounterTemplatePath = "testdata/share-counter-template"
	// shareCounterRate is the total rate of each CPU's group: (8640.25 + 4320.5 + 73439.25) / 86400.
	shareCounterRate = 1.0
	// shareCounterTolerance covers the rounding of three members to 2 decimals.
	shareCounterTolerance = 3*0.005 + 1e-6
)

var cpus = []string{"cpu=cpu0", "cpu=cpu1"}

// assertShareCounterBatches checks the share counter template's properties for the batches of
// one host, using the data points' own timestamps and start timestamps.
func assertShareCounterBatches(t *testing.T, host string, batches []pmetric.Metrics) {
	t.Helper()
	var previous map[string]map[string]float64
	var previousTime time.Time
	rates := map[float64]struct{}{}
	for i, batch := range batches {
		timestamp := batchTimestamp(t, batch)
		start := batchStartTimestamp(t, batch)
		elapsed := timestamp.Sub(start).Seconds()
		groups := shareGroupValues(t, batch, "state")
		for _, cpu := range cpus {
			counter := groups["system.cpu.time|"+cpu]
			utilization := groups["system.cpu.utilization|"+cpu]
			require.Len(t, counter, 3)
			// The group adds up to the total rate times the time since the start.
			require.InDelta(t, shareCounterRate*elapsed, sumValues(counter), shareCounterTolerance, "%s sample %d %s", host, i, cpu)
			require.InDelta(t, 1, sumValues(utilization), 0.01+1e-9, "%s sample %d %s", host, i, cpu)
			if previous == nil || !batchStartTimestamp(t, batches[i-1]).Equal(start) {
				continue
			}
			interval := timestamp.Sub(previousTime).Seconds()
			for state, value := range counter {
				before := previous["system.cpu.time|"+cpu][state]
				require.GreaterOrEqual(t, value, before, "%s sample %d %s %s", host, i, cpu, state)
				// The rate share over the interval is about the mean of the utilization at its
				// ends (same variation key): the trapezoid rule, plus rounding.
				rateShare := (value - before) / (shareCounterRate * interval)
				gaugeShare := (utilization[state] + previous["system.cpu.utilization|"+cpu][state]) / 2
				require.InDelta(t, gaugeShare, rateShare, 0.03, "%s sample %d %s %s", host, i, cpu, state)
				if state == "user" {
					rates[rateShare] = struct{}{}
				}
			}
		}
		previous = groups
		previousTime = timestamp
	}
	// The rates vary over time.
	assert.Greater(t, len(rates), 20, host)
}

func TestShareCounterGeneratedInstances(t *testing.T) {
	const intervals = 360
	outputs := map[string]map[string]string{}
	for _, tc := range []struct {
		name        string
		scale       int
		concurrency int
	}{
		{name: "scale 4", scale: 4},
		{name: "scale 4 concurrency 2", scale: 4, concurrency: 2},
		{name: "scale 2", scale: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := modelTestConfig(shareCounterTemplatePath, intervals, func(scn *ScenarioCfg) {
				scn.Scale = tc.scale
				scn.Concurrency = tc.concurrency
			})
			require.NoError(t, cfg.Validate())
			allMetrics := runReceiverToCompletion(t, cfg)
			requireNoHintMetadata(t, allMetrics)
			byHost := batchesByHost(t, allMetrics)
			require.Len(t, byHost, tc.scale)
			for host, batches := range byHost {
				require.Len(t, batches, intervals)
				// The first sample is at the receiver's start time: everything is 0.
				assert.Zero(t, sumValues(shareGroupValues(t, batches[0], "state")["system.cpu.time|cpu=cpu0"]), host)
				assertShareCounterBatches(t, host, batches)
			}
			outputs[tc.name] = marshalByHost(t, allMetrics)
		})
	}
	// The values of an instance depend on neither the concurrency nor the other instances.
	assert.Equal(t, outputs["scale 4"], outputs["scale 4 concurrency 2"])
	for host, output := range outputs["scale 2"] {
		assert.Equal(t, outputs["scale 4"][host], output, host)
	}
	assert.NotEqual(t, outputs["scale 4"]["host-0"], outputs["scale 4"]["host-1"])
}

func TestShareCounterExplicitInstancesAndRestarts(t *testing.T) {
	const intervals = 120
	instances := map[string]InstanceCfg{
		"steady": {ResourceAttributes: map[string]any{"host.name": "steady"}},
		// Started long before the window: the counters already hold a day of CPU time.
		"stamped": {
			ResourceAttributes: map[string]any{"host.name": "stamped"},
			StartTimestamp:     timePtr(modelTestStart.Add(-24 * time.Hour)),
			IntervalOffset:     durationPtr(3 * time.Second),
		},
		// Restarts between ticks, twice.
		"restarting": {
			ResourceAttributes: map[string]any{"host.name": "restarting"},
			Restarts:           []time.Time{modelTestStart.Add(95 * time.Second), modelTestStart.Add(10*time.Minute + 1500*time.Millisecond)},
		},
	}
	config := func(hosts ...string) *Config {
		return modelTestConfig(shareCounterTemplatePath, intervals, func(scn *ScenarioCfg) {
			for _, host := range hosts {
				scn.Instances = append(scn.Instances, instances[host])
			}
		})
	}
	withJitter := func(cfg *Config) *Config {
		cfg.IntervalJitterStdDev = 2 * time.Second
		return cfg
	}
	// share_counter keeps no counter baseline across restarts.
	rcv, err := newMetricsGenReceiver(config("restarting"), receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	assert.Equal(t, baselineSize{}, rcv.scenarios[0].resetSlotCounts)

	all := runReceiverToCompletion(t, withJitter(config("steady", "stamped", "restarting")))
	requireNoHintMetadata(t, all)
	byHost := batchesByHost(t, all)
	require.Len(t, byHost, 3)
	for host, batches := range byHost {
		require.Len(t, batches, intervals)
		assertShareCounterBatches(t, host, batches)
	}

	// After each restart, the start timestamp moves to the restart and the group counts from
	// there: R * (t - restart) right from the first sample after it.
	restarting := byHost["restarting"]
	for _, restart := range instances["restarting"].Restarts {
		first := -1
		for i, batch := range restarting {
			if batchStartTimestamp(t, batch).Equal(restart) {
				first = i
				break
			}
		}
		require.Positive(t, first, restart)
		before := sumValues(shareGroupValues(t, restarting[first-1], "state")["system.cpu.time|cpu=cpu0"])
		after := sumValues(shareGroupValues(t, restarting[first], "state")["system.cpu.time|cpu=cpu0"])
		elapsed := batchTimestamp(t, restarting[first]).Sub(restart).Seconds()
		assert.Positive(t, elapsed)
		assert.InDelta(t, shareCounterRate*elapsed, after, shareCounterTolerance, restart)
		assert.Less(t, after, before, restart)
	}
	stamped := sumValues(shareGroupValues(t, byHost["stamped"][0], "state")["system.cpu.time|cpu=cpu1"])
	// A day, the interval offset and the jitter.
	assert.GreaterOrEqual(t, stamped, 86400+3-shareCounterTolerance)
	assert.Less(t, stamped, 86400.0+10)

	// A run over a subset of the instances emits the same for them.
	subset := runReceiverToCompletion(t, withJitter(config("restarting", "steady")))
	allByHost := marshalByHost(t, all)
	subsetByHost := marshalByHost(t, subset)
	require.Len(t, subsetByHost, 2)
	for host, output := range subsetByHost {
		assert.Equal(t, allByHost[host], output, host)
	}
}

func TestShareCounterRejectsDeltaTemporality(t *testing.T) {
	cfg := modelTestConfig(shareCounterTemplatePath, 1, func(scn *ScenarioCfg) {
		scn.Scale = 1
		scn.TemporalityOverride = "delta"
	})
	_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.EqualError(t, err, shareCounterTemplatePath+`: metric "system.cpu.time" with generation hint share_counter: the metric must be a cumulative monotonic sum, but temporality_override makes it delta`)
}
