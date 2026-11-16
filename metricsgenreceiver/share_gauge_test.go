package metricsgenreceiver

import (
	"sort"
	"strings"
	"testing"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const shareGaugeTemplatePath = "testdata/share-gauge-template"

// shareGroupValues returns the values of the number data points of md by group (the metric name
// and the other attributes) and by the value of the shareBy attribute.
func shareGroupValues(t *testing.T, md pmetric.Metrics, shareBy string) map[string]map[string]float64 {
	t.Helper()
	groups := map[string]map[string]float64{}
	dp.ForEachDataPoint(&md, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		var others []string
		member := ""
		dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == shareBy {
				member = v.AsString()
			} else {
				others = append(others, k+"="+v.AsString())
			}
			return true
		})
		sort.Strings(others)
		key := m.Name() + "|" + strings.Join(others, ",")
		if groups[key] == nil {
			groups[key] = map[string]float64{}
		}
		require.NotContains(t, groups[key], member)
		groups[key][member] = numberValue(dataPoint.(pmetric.NumberDataPoint))
	})
	return groups
}

func sumValues(values map[string]float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum
}

// assertShareGaugeBatches checks the share gauge template's properties for the batches of one
// host.
func assertShareGaugeBatches(t *testing.T, host string, batches []pmetric.Metrics) {
	t.Helper()
	ranges := map[string]*seriesRange{}
	for i, batch := range batches {
		groups := shareGroupValues(t, batch, "state")
		require.Len(t, groups, 4)
		usage := groups["system.memory.usage|"]
		utilization := groups["system.memory.utilization|"]
		// Exact for ints; within one unit of the last decimal (1 for the memory utilization, 2
		// for the CPU) for doubles.
		require.Equal(t, 1e10, sumValues(usage), "%s sample %d", host, i)
		require.InDelta(t, 1, sumValues(utilization), 0.1+1e-9, "%s sample %d", host, i)
		require.InDelta(t, 1, sumValues(groups["system.cpu.utilization|cpu=cpu0"]), 0.01+1e-9, "%s sample %d", host, i)
		require.InDelta(t, 1, sumValues(groups["system.cpu.utilization|cpu=cpu1"]), 0.01+1e-9, "%s sample %d", host, i)
		require.Zero(t, usage["buffered"])
		require.Zero(t, utilization["buffered"])
		for state, value := range usage {
			require.GreaterOrEqual(t, value, 0.0)
			// The same variation key: usage over its total is the utilization, up to the
			// utilization's rounding to one decimal and the residual added to its largest member.
			require.InDelta(t, value/1e10, utilization[state], 0.15+1e-3, "%s sample %d %s", host, i, state)
		}
		for key, values := range groups {
			for state, value := range values {
				require.GreaterOrEqual(t, value, 0.0)
				if ranges[key+state] == nil {
					ranges[key+state] = &seriesRange{}
				}
				ranges[key+state].add(value)
			}
		}
	}
	// The members move, also between the cpus.
	assert.Greater(t, ranges["system.memory.usage|used"].distinct(), 100, host)
	assert.Greater(t, ranges["system.cpu.utilization|cpu=cpu0user"].distinct(), 2, host)
	assert.Greater(t, ranges["system.cpu.utilization|cpu=cpu0idle"].max-ranges["system.cpu.utilization|cpu=cpu0idle"].min, 0.02, host)
}

func TestShareGaugeGeneratedInstances(t *testing.T) {
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
			cfg := modelTestConfig(shareGaugeTemplatePath, intervals, func(scn *ScenarioCfg) {
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
				assertShareGaugeBatches(t, host, batches)
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

func TestShareGaugeExplicitInstances(t *testing.T) {
	const intervals = 120
	instance := func(host string) InstanceCfg {
		return InstanceCfg{ResourceAttributes: map[string]any{"host.name": host}}
	}
	all := runReceiverToCompletion(t, modelTestConfig(shareGaugeTemplatePath, intervals, func(scn *ScenarioCfg) {
		scn.Instances = []InstanceCfg{instance("a"), instance("b"), instance("c")}
	}))
	subset := runReceiverToCompletion(t, modelTestConfig(shareGaugeTemplatePath, intervals, func(scn *ScenarioCfg) {
		scn.Instances = []InstanceCfg{instance("b")}
	}))
	requireNoHintMetadata(t, all)
	for host, batches := range batchesByHost(t, all) {
		assertShareGaugeBatches(t, host, batches)
	}
	allByHost := marshalByHost(t, all)
	assert.Equal(t, allByHost["b"], marshalByHost(t, subset)["b"])
	assert.NotEqual(t, allByHost["a"], allByHost["b"])

	// The mean shares differ between instances.
	first := map[string]float64{}
	for host, batches := range batchesByHost(t, all) {
		first[host] = shareGroupValues(t, batches[0], "state")["system.memory.usage|"]["used"]
	}
	assert.Len(t, uniqueFloats(first), 3)
}

func TestShareGaugeStartupErrors(t *testing.T) {
	cfg := modelTestConfig("", 1, func(scn *ScenarioCfg) { scn.Scale = 1 })
	_, err := newMetricsGenReceiverFromTemplate(t, cfg, `
          - name: test.share
            gauge:
              dataPoints:
                - { asInt: "1", attributes: [ { key: state, value: { stringValue: a } } ] }
                - { asInt: "2", attributes: [ { key: cpu, value: { stringValue: cpu0 } } ] }
            metadata:
              - { key: metricsgen.hint.class, value: { stringValue: share_gauge } }
              - { key: metricsgen.hint.share_by, value: { stringValue: state } }
`)
	require.ErrorContains(t, err, `metric "test.share" with generation hint share_gauge: the data point with attributes {cpu=cpu0} has no attribute "state" named by metricsgen.hint.share_by`)

	_, err = newMetricsGenReceiverFromTemplate(t, cfg, `
          - name: test.share
            gauge:
              dataPoints:
                - { asInt: "1", attributes: [ { key: state, value: { stringValue: a } } ] }
            metadata:
              - { key: metricsgen.hint.class, value: { stringValue: share_gauge } }
`)
	require.ErrorContains(t, err, `metric "test.share" with generation hint share_gauge: parameter metricsgen.hint.share_by is required`)
}

func uniqueFloats(values map[string]float64) map[float64]struct{} {
	unique := map[float64]struct{}{}
	for _, v := range values {
		unique[v] = struct{}{}
	}
	return unique
}
