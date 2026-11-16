package metricsgenreceiver

import (
	"os"
	"strings"
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
	boundedGaugeTemplatePath = "testdata/bounded-gauge-template"
	modelTestInterval        = 10 * time.Second
)

var modelTestStart = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func modelTestConfig(path string, intervals int, scenario func(*ScenarioCfg)) *Config {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.StartTime = modelTestStart
	cfg.Interval = modelTestInterval
	cfg.EndTime = cfg.StartTime.Add(time.Duration(intervals) * modelTestInterval)
	cfg.Seed = 9
	scn := ScenarioCfg{Path: path}
	if scenario != nil {
		scenario(&scn)
	}
	cfg.Scenarios = []ScenarioCfg{scn}
	return cfg
}

// requireNoHintMetadata fails if any emitted metric still carries hint metadata.
func requireNoHintMetadata(t *testing.T, allMetrics []pmetric.Metrics) {
	t.Helper()
	for _, md := range allMetrics {
		dp.ForEachMetric(&md, func(_ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric) {
			m.Metadata().Range(func(key string, _ pcommon.Value) bool {
				require.False(t, strings.HasPrefix(key, "metricsgen.hint."), "%s: %s", m.Name(), key)
				return true
			})
		})
	}
}

// marshalByHost serialises the batches of every host, in emission order.
func marshalByHost(t *testing.T, allMetrics []pmetric.Metrics) map[string]string {
	t.Helper()
	out := map[string]string{}
	for host, batches := range batchesByHost(t, allMetrics) {
		out[host] = marshalMetricsOutput(t, batches).String()
	}
	return out
}

func TestBoundedGaugeGeneratedInstances(t *testing.T) {
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
			cfg := modelTestConfig(boundedGaugeTemplatePath, intervals, func(scn *ScenarioCfg) {
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
				load := seriesRange{}
				usage := seriesRange{}
				for _, batch := range batches {
					values := seriesValues(t, batch)
					load.add(values["system.cpu.load_average.1m"])
					usage.add(values["system.memory.usage|used"])
					utilization := values["system.cpu.utilization|cpu0"]
					require.LessOrEqual(t, utilization, 1.0, host)
					require.GreaterOrEqual(t, utilization, 0.85/2*0.5, host)
				}
				// The centre is within [seed/2, seed*2] and values stay within variation of it.
				assert.GreaterOrEqual(t, load.min, 1.5/2*0.75-0.05, host)
				assert.LessOrEqual(t, load.max, 1.5*2*1.25+0.05, host)
				assert.LessOrEqual(t, load.max-load.min, 2*0.25*load.max/0.75+0.1, host)
				assert.Greater(t, load.distinct(), 5, host)
				assert.GreaterOrEqual(t, usage.min, 4e9/2*0.9, host)
				assert.LessOrEqual(t, usage.max, 4e9*2*1.1, host)
				assert.LessOrEqual(t, usage.max-usage.min, 2*0.1*usage.max/0.9, host)
				assert.Greater(t, usage.distinct(), 100, host)
				for _, batch := range batches {
					dp.ForEachDataPoint(&batch, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
						if m.Name() == "system.memory.usage" {
							require.Equal(t, pmetric.NumberDataPointValueTypeInt, dataPoint.(pmetric.NumberDataPoint).ValueType())
						}
					})
				}
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

func TestBoundedGaugeExplicitInstances(t *testing.T) {
	const intervals = 60
	instance := func(host string) InstanceCfg {
		return InstanceCfg{ResourceAttributes: map[string]any{"host.name": host}}
	}
	all := runReceiverToCompletion(t, modelTestConfig(boundedGaugeTemplatePath, intervals, func(scn *ScenarioCfg) {
		scn.Instances = []InstanceCfg{instance("a"), instance("b"), instance("c")}
	}))
	subset := runReceiverToCompletion(t, modelTestConfig(boundedGaugeTemplatePath, intervals, func(scn *ScenarioCfg) {
		scn.Instances = []InstanceCfg{instance("c"), instance("a")}
	}))
	requireNoHintMetadata(t, all)
	allByHost := marshalByHost(t, all)
	subsetByHost := marshalByHost(t, subset)
	require.Len(t, subsetByHost, 2)
	for host, output := range subsetByHost {
		assert.Equal(t, allByHost[host], output, host)
	}
	assert.NotEqual(t, allByHost["a"], allByHost["b"])
	for host, batches := range batchesByHost(t, all) {
		load := seriesRange{}
		for _, batch := range batches {
			load.add(seriesValues(t, batch)["system.cpu.load_average.1m"])
		}
		assert.Greater(t, load.distinct(), 5, host)
	}
}

func TestBoundedGaugeStartupErrors(t *testing.T) {
	cfg := modelTestConfig("testdata/lifecycle-template", 1, func(scn *ScenarioCfg) { scn.Scale = 1 })
	_, err := newMetricsGenReceiverFromTemplate(t, cfg, `
          - name: test.counter
            sum:
              aggregationTemporality: 2
              isMonotonic: true
              dataPoints:
                - { asInt: "10" }
            metadata:
              - { key: metricsgen.hint.class, value: { stringValue: bounded_gauge } }
`)
	require.ErrorContains(t, err, `metric "test.counter" with generation hint bounded_gauge: the metric must be a gauge or a non-monotonic sum`)

	_, err = newMetricsGenReceiverFromTemplate(t, cfg, `
          - name: test.gauge
            gauge:
              dataPoints:
                - { asInt: "10" }
            metadata:
              - { key: metricsgen.hint.class, value: { stringValue: bounded_gauge } }
              - { key: metricsgen.hint.variation, value: { stringValue: "2" } }
`)
	require.ErrorContains(t, err, `metric "test.gauge" with generation hint bounded_gauge: parameter metricsgen.hint.variation must be a number in [0, 1], got "2"`)
}

// newMetricsGenReceiverFromTemplate writes a template with the given metrics (YAML list items of
// one scope) and creates a receiver for it with cfg's first scenario pointing at it.
func newMetricsGenReceiverFromTemplate(t *testing.T, cfg *Config, metrics string) (*MetricsGenReceiver, error) {
	t.Helper()
	path := t.TempDir() + "/template"
	require.NoError(t, os.WriteFile(path+".yaml", []byte("resourceMetrics:\n  - scopeMetrics:\n      - metrics:"+metrics), 0o600))
	require.NoError(t, os.WriteFile(path+"-resource-attributes.yaml", []byte("resourceMetrics:\n  - resource: {}\n"), 0o600))
	cfg.Scenarios[0].Path = path
	return newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
}

// seriesRange tracks the range and distinct values of a series.
type seriesRange struct {
	min, max float64
	values   map[float64]struct{}
}

func (r *seriesRange) add(v float64) {
	if r.values == nil {
		r.min, r.max, r.values = v, v, map[float64]struct{}{}
	}
	r.min, r.max = min(r.min, v), max(r.max, v)
	r.values[v] = struct{}{}
}

func (r *seriesRange) distinct() int {
	return len(r.values)
}
