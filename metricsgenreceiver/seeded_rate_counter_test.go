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
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const (
	seededRateTemplatePath = "testdata/seeded-rate-template"
	seededRateInterval     = 10 * time.Second
	// seededRateGoldenSHA256 is the SHA-256 of the output of TestSeededRateCounterDeterministic.
	// The fixture only uses values that are computed identically on arm64 and amd64.
	seededRateGoldenSHA256 = "2101c66ce2b6a1587cc900b249e5b5d63d33998562552c5031590a315da98282"
)

// seededRateSeries are the series of the seeded rate template with a non-zero rate, keyed like
// seriesValues, with their seed value and their rate per second.
var seededRateSeries = []struct {
	key  string
	seed float64
	rate float64
}{
	{key: "system.cpu.time|idle", seed: 691200.5, rate: 691200.5 / 86400},
	{key: "system.cpu.time|steal", seed: 345.6, rate: 0.004},
	{key: "system.network.io|eth0", seed: 8640000000, rate: 100000},
	{key: "system.network.io|lo", seed: 2592, rate: 0.03},
}

func seededRateConfig(intervals int) *Config {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.StartTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg.Interval = seededRateInterval
	cfg.EndTime = cfg.StartTime.Add(time.Duration(intervals) * seededRateInterval)
	cfg.Seed = 5
	cfg.Scenarios = []ScenarioCfg{{Path: seededRateTemplatePath, Scale: 3}}
	return cfg
}

func TestSeededRateCounter(t *testing.T) {
	const intervals = 4000
	cfg := seededRateConfig(intervals)
	require.NoError(t, cfg.Validate())
	sink := new(consumertest.MetricsSink)
	rcv, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(typ), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), nil))
	waitForCompletion(rcv)
	require.NoError(t, rcv.Shutdown(context.Background()))

	template := seriesValues(t, *rcv.(*MetricsGenReceiver).scenarios[0].metricsTemplate)
	byHost := batchesByHost(t, sink.AllMetrics())
	require.Len(t, byHost, 3)
	for _, series := range seededRateSeries {
		want := series.rate * seededRateInterval.Seconds()
		// The shared template grows at the seeded rate.
		assert.InEpsilon(t, want, (template[series.key]-series.seed)/intervals, 0.05, series.key)

		factors := map[float64]struct{}{}
		for host, batches := range byHost {
			require.Len(t, batches, intervals)
			first := seriesValues(t, batches[0])[series.key]
			last := seriesValues(t, batches[len(batches)-1])[series.key]
			// Each instance grows at a fixed multiple of the template's rate.
			factor := last / template[series.key]
			assert.GreaterOrEqual(t, factor, 0.5, "%s %s", host, series.key)
			assert.Less(t, factor, 1.5, "%s %s", host, series.key)
			factors[factor] = struct{}{}
			assert.InEpsilon(t, factor*want, (last-first)/(intervals-1), 0.05, "%s %s", host, series.key)
		}
		assert.Len(t, factors, 3, "instances differ: %s", series.key)
	}
	// Two series of the same metric grow at their own rates.
	assert.Greater(t, template["system.cpu.time|idle"]-691200.5, 1000*(template["system.cpu.time|steal"]-345.6))

	for host, batches := range byHost {
		previous := seriesValues(t, batches[0])
		for _, batch := range batches[1:] {
			values := seriesValues(t, batch)
			for key, value := range values {
				require.GreaterOrEqual(t, value, previous[key], "%s %s", host, key)
			}
			require.Zero(t, values["system.network.errors|eth0"], host)
			previous = values
		}
	}
}

func TestSeededRateCounterDeltaTemporality(t *testing.T) {
	const intervals = 2000
	cfg := seededRateConfig(intervals)
	cfg.Scenarios[0].TemporalityOverride = "delta"
	byHost := batchesByHost(t, runReceiverToCompletion(t, cfg))
	require.Len(t, byHost, 3)
	hosts := []string{"host-0", "host-1", "host-2"}
	for _, series := range seededRateSeries[:3] {
		want := series.rate * seededRateInterval.Seconds()
		for _, host := range hosts {
			batches := byHost[host]
			require.Len(t, batches, intervals)
			total := 0.0
			for i, batch := range batches {
				value := seriesValues(t, batch)[series.key]
				require.GreaterOrEqual(t, value, 0.0)
				total += value
				// The per-instance factor is the same at every interval.
				reference := seriesValues(t, byHost[hosts[0]][i])[series.key]
				if reference > 0 && i > 0 {
					previousReference := seriesValues(t, byHost[hosts[0]][i-1])[series.key]
					previous := seriesValues(t, batches[i-1])[series.key]
					if previousReference > 0 {
						assert.InEpsilon(t, previous/previousReference, value/reference, 0.01, "%s %s %d", host, series.key, i)
					}
				}
			}
			average := total / intervals
			assert.GreaterOrEqual(t, average, 0.5*0.95*want, "%s %s", host, series.key)
			assert.Less(t, average, 1.5*1.05*want, "%s %s", host, series.key)
		}
	}
}

func TestSeededRateCounterRestart(t *testing.T) {
	cfg := lifecycleConfig(lifecycleInstance("restarting", func(c *InstanceCfg) { c.Restarts = []time.Time{*at(2 * time.Minute)} }))
	cfg.Scenarios[0].Path = seededRateTemplatePath
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	assert.Equal(t, baselineSize{ints: 3, doubles: 2}, rcv.scenarios[0].resetSlotCounts)

	batches := runReceiverToCompletion(t, cfg)
	require.Len(t, batches, 10)
	// The restart is at sample 4: the counters read 0 there and increase from there.
	for i := 4; i < len(batches); i++ {
		values := seriesValues(t, batches[i])
		previous := seriesValues(t, batches[i-1])
		for _, series := range seededRateSeries {
			if i == 4 {
				assert.Zero(t, values[series.key], series.key)
				assert.Positive(t, previous[series.key], series.key)
			} else {
				assert.GreaterOrEqual(t, values[series.key], previous[series.key], "sample %d %s", i, series.key)
			}
		}
	}
	last := seriesValues(t, batches[9])
	assert.Positive(t, last["system.cpu.time|idle"])
	assert.Positive(t, last["system.network.io|eth0"])
}

func TestSeededRateCounterDeterministic(t *testing.T) {
	first := marshalMetricsOutput(t, runReceiverToCompletion(t, seededRateConfig(30)))
	second := marshalMetricsOutput(t, runReceiverToCompletion(t, seededRateConfig(30)))
	require.Equal(t, first.String(), second.String())
	sum := sha256.Sum256(first.Bytes())
	assert.Equal(t, seededRateGoldenSHA256, hex.EncodeToString(sum[:]))
}

func TestSeededRateCounterStartupErrors(t *testing.T) {
	tests := []struct {
		name      string
		dataPoint string
		wantError string
	}{
		{
			name:      "no start timestamp",
			dataPoint: `{ timeUnixNano: "1734393600000000000", asInt: "10" }`,
			wantError: `metric "test.seeded" with generation hint seeded_rate_counter: the data point needs both startTimeUnixNano and timeUnixNano`,
		},
		{
			name:      "timestamp not after start",
			dataPoint: `{ startTimeUnixNano: "1734393600000000000", timeUnixNano: "1734393600000000000", asInt: "10" }`,
			wantError: `metric "test.seeded" with generation hint seeded_rate_counter: timeUnixNano (1734393600000000000) must be after startTimeUnixNano (1734393600000000000)`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "seeded")
			require.NoError(t, os.WriteFile(path+".yaml", []byte(`resourceMetrics:
  - scopeMetrics:
      - metrics:
          - name: test.seeded
            sum:
              aggregationTemporality: 2
              isMonotonic: true
              dataPoints:
                - `+tc.dataPoint+`
            metadata:
              - { key: metricsgen.hint.class, value: { stringValue: seeded_rate_counter } }
`), 0o600))
			require.NoError(t, os.WriteFile(path+"-resource-attributes.yaml", []byte("resourceMetrics:\n  - resource: {}\n"), 0o600))
			cfg := seededRateConfig(1)
			cfg.Scenarios[0].Path = path
			_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
			require.EqualError(t, err, path+": "+tc.wantError)
		})
	}
}

// seriesValues returns the value of every number data point, keyed by the metric name and the
// value of its first attribute.
func seriesValues(t *testing.T, md pmetric.Metrics) map[string]float64 {
	t.Helper()
	values := map[string]float64{}
	dp.ForEachDataPoint(&md, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		key := m.Name()
		dataPoint.Attributes().Range(func(_ string, v pcommon.Value) bool {
			key += "|" + v.AsString()
			return false
		})
		require.NotContains(t, values, key)
		values[key] = numberValue(dataPoint.(pmetric.NumberDataPoint))
	})
	return values
}
