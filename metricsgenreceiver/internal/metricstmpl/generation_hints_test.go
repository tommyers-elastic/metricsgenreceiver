package metricstmpl

import (
	"testing"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/distribution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestExtractGenerationHintsNoMetadataIsNoop(t *testing.T) {
	metrics := pmetric.NewMetrics()
	appendGaugeMetric(metrics, "test.metric")

	hints, err := ExtractGenerationHints(&metrics)
	require.NoError(t, err)
	assert.Empty(t, hints)
}

func TestExtractGenerationHintsReadsAndStripsMetadata(t *testing.T) {
	metrics := pmetric.NewMetrics()
	m := appendGaugeMetric(metrics, "test.metric")
	m.Metadata().PutStr(GenerationHintMetadataKey, "slow_gauge")
	m.Metadata().PutStr("prometheus.type", "gauge")

	hints, err := ExtractGenerationHints(&metrics)
	require.NoError(t, err)
	require.Len(t, hints, 1)
	assert.Equal(t, "slow_gauge", string(hints["test.metric"].Class))

	// Hint metadata is stripped; unrelated metadata is left alone.
	_, hasHint := m.Metadata().Get(GenerationHintMetadataKey)
	assert.False(t, hasHint)
	_, hasPromType := m.Metadata().Get("prometheus.type")
	assert.True(t, hasPromType)
}

func TestExtractGenerationHintsInvalidClass(t *testing.T) {
	metrics := pmetric.NewMetrics()
	m := appendGaugeMetric(metrics, "test.metric")
	m.Metadata().PutStr(GenerationHintMetadataKey, "not_a_real_hint")

	hints, err := ExtractGenerationHints(&metrics)
	require.Error(t, err)
	assert.Nil(t, hints)
	assert.Contains(t, err.Error(), "unsupported class")
	assert.Contains(t, err.Error(), "test.metric")

	// Metadata is not stripped on error so the failure stays inspectable.
	v, ok := m.Metadata().Get(GenerationHintMetadataKey)
	require.True(t, ok)
	assert.Equal(t, "not_a_real_hint", v.Str())
}

func TestExtractGenerationHintsMissingClass(t *testing.T) {
	metrics := pmetric.NewMetrics()
	m := appendGaugeMetric(metrics, "test.metric")
	m.Metadata().PutStr(GenerationHintMetadataKey, "")

	hints, err := ExtractGenerationHints(&metrics)
	require.Error(t, err)
	assert.Nil(t, hints)
	assert.Contains(t, err.Error(), "missing a class")
	assert.Contains(t, err.Error(), "test.metric")
}

func TestExtractGenerationHintsNonStringMetadata(t *testing.T) {
	metrics := pmetric.NewMetrics()
	m := appendGaugeMetric(metrics, "test.metric")
	m.Metadata().PutInt(GenerationHintMetadataKey, 42)

	hints, err := ExtractGenerationHints(&metrics)
	require.Error(t, err)
	assert.Nil(t, hints)
	assert.Contains(t, err.Error(), "non-string")
}

func TestExtractGenerationHintsRepeatedMetricNameSameHintIsAllowed(t *testing.T) {
	metrics := pmetric.NewMetrics()
	first := appendGaugeMetric(metrics, "test.metric")
	second := appendGaugeMetric(metrics, "test.metric")
	first.Metadata().PutStr(GenerationHintMetadataKey, "steady_counter")
	second.Metadata().PutStr(GenerationHintMetadataKey, "steady_counter")

	hints, err := ExtractGenerationHints(&metrics)
	require.NoError(t, err)
	require.Len(t, hints, 1)
	assert.Equal(t, "steady_counter", string(hints["test.metric"].Class))

	_, firstHasHint := first.Metadata().Get(GenerationHintMetadataKey)
	_, secondHasHint := second.Metadata().Get(GenerationHintMetadataKey)
	assert.False(t, firstHasHint)
	assert.False(t, secondHasHint)
}

func TestExtractGenerationHintsRepeatedMetricNameDifferentHintFails(t *testing.T) {
	metrics := pmetric.NewMetrics()
	first := appendGaugeMetric(metrics, "test.metric")
	second := appendGaugeMetric(metrics, "test.metric")
	first.Metadata().PutStr(GenerationHintMetadataKey, "steady_counter")
	second.Metadata().PutStr(GenerationHintMetadataKey, "slow_gauge")

	hints, err := ExtractGenerationHints(&metrics)
	require.Error(t, err)
	assert.Nil(t, hints)
	assert.Contains(t, err.Error(), "same metricsgen.hint.class across all occurrences")

	v, ok := first.Metadata().Get(GenerationHintMetadataKey)
	require.True(t, ok)
	assert.Equal(t, "steady_counter", v.Str())
}

func TestExtractGenerationHintsRepeatedMetricNameMixedHintPresenceFails(t *testing.T) {
	metrics := pmetric.NewMetrics()
	first := appendGaugeMetric(metrics, "test.metric")
	appendGaugeMetric(metrics, "test.metric")
	first.Metadata().PutStr(GenerationHintMetadataKey, "steady_counter")

	hints, err := ExtractGenerationHints(&metrics)
	require.Error(t, err)
	assert.Nil(t, hints)
	assert.Contains(t, err.Error(), "declare metricsgen.hint.class consistently across all occurrences")
}

func TestExtractGenerationHintsRejectsNonNumberMetrics(t *testing.T) {
	tests := []struct {
		name   string
		append func(pmetric.Metrics, string) pmetric.Metric
	}{
		{
			name:   "histogram",
			append: appendHistogramMetric,
		},
		{
			name:   "exponential histogram",
			append: appendExponentialHistogramMetric,
		},
		{
			name:   "summary",
			append: appendSummaryMetric,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := pmetric.NewMetrics()
			m := tt.append(metrics, "test.metric")
			m.Metadata().PutStr(GenerationHintMetadataKey, "constant")

			hints, err := ExtractGenerationHints(&metrics)
			require.Error(t, err)
			assert.Nil(t, hints)
			assert.Contains(t, err.Error(), "must be a gauge or sum")
		})
	}
}

func TestExtractGenerationHintsReadsAndStripsParameters(t *testing.T) {
	metrics := pmetric.NewMetrics()
	first := appendGaugeMetric(metrics, "test.metric")
	second := appendGaugeMetric(metrics, "test.metric")
	for _, m := range []pmetric.Metric{first, second} {
		m.Metadata().PutStr(GenerationHintMetadataKey, "bounded_gauge")
		m.Metadata().PutStr("metricsgen.hint.variation", "0.1")
		m.Metadata().PutStr("metricsgen.hint.period", "1h")
		m.Metadata().PutStr("prometheus.type", "gauge")
	}

	hints, err := ExtractGenerationHints(&metrics)
	require.NoError(t, err)
	assert.Equal(t, distribution.MetricGenerationHint{
		Class:  distribution.GenerationHintBoundedGauge,
		Params: map[string]string{"variation": "0.1", "period": "1h"},
	}, hints["test.metric"])
	for _, m := range []pmetric.Metric{first, second} {
		assert.Equal(t, map[string]any{"prometheus.type": "gauge"}, m.Metadata().AsRaw())
	}
}

func TestExtractGenerationHintsParameterErrors(t *testing.T) {
	tests := []struct {
		name      string
		metadata  []map[string]any
		wantError string
	}{
		{
			name:      "unknown parameter",
			metadata:  []map[string]any{{"metricsgen.hint.class": "bounded_gauge", "metricsgen.hint.spread": "2"}},
			wantError: `metric "test.metric" with generation hint bounded_gauge: unknown parameter metricsgen.hint.spread`,
		},
		{
			name:      "bad value",
			metadata:  []map[string]any{{"metricsgen.hint.class": "bounded_gauge", "metricsgen.hint.period": "soon"}},
			wantError: `metric "test.metric" with generation hint bounded_gauge: parameter metricsgen.hint.period must be a positive duration of at most 8760h0m0s, got "soon"`,
		},
		{
			name:      "non-string parameter",
			metadata:  []map[string]any{{"metricsgen.hint.class": "bounded_gauge", "metricsgen.hint.variation": 0.1}},
			wantError: `metric "test.metric" has non-string metricsgen.hint.variation metadata`,
		},
		{
			name: "different parameters across occurrences",
			metadata: []map[string]any{
				{"metricsgen.hint.class": "bounded_gauge", "metricsgen.hint.variation": "0.1"},
				{"metricsgen.hint.class": "bounded_gauge", "metricsgen.hint.variation": "0.2"},
			},
			wantError: `metric "test.metric" must declare the same hint parameters across all occurrences`,
		},
		{
			name: "parameters missing on one occurrence",
			metadata: []map[string]any{
				{"metricsgen.hint.class": "bounded_gauge", "metricsgen.hint.variation": "0.1"},
				{"metricsgen.hint.class": "bounded_gauge"},
			},
			wantError: `metric "test.metric" must declare the same hint parameters across all occurrences`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metrics := pmetric.NewMetrics()
			var first pmetric.Metric
			for i, metadata := range tc.metadata {
				m := appendGaugeMetric(metrics, "test.metric")
				require.NoError(t, m.Metadata().FromRaw(metadata))
				if i == 0 {
					first = m
				}
			}
			hints, err := ExtractGenerationHints(&metrics)
			require.EqualError(t, err, tc.wantError)
			assert.Nil(t, hints)
			// Metadata is not stripped on error.
			assert.Equal(t, tc.metadata[0], first.Metadata().AsRaw())
		})
	}
}

// classesWithoutParams are the classes that take no parameters. For them, and for metrics
// without a class, only metricsgen.hint.class is hint metadata.
var classesWithoutParams = []string{"clock", "constant", "current_count", "slow_gauge", "seeded_rate_counter", "sparse_counter", "stable_binary", "steady_counter"}

func TestExtractGenerationHintsClassesWithoutParamsPassOtherKeysThrough(t *testing.T) {
	passThrough := map[string]any{
		"metricsgen.hint.foo":       "x",
		"metricsgen.hint.bar":       int64(7),
		"metricsgen.hint.variation": "0.1",
		"prometheus.type":           "gauge",
	}
	for _, class := range classesWithoutParams {
		t.Run(class, func(t *testing.T) {
			require.False(t, distribution.GenerationHintClass(class).TakesParams())
			metrics := pmetric.NewMetrics()
			first := appendGaugeMetric(metrics, "test.metric")
			second := appendGaugeMetric(metrics, "test.metric")
			require.NoError(t, first.Metadata().FromRaw(passThrough))
			first.Metadata().PutStr(GenerationHintMetadataKey, class)
			// The other keys need not agree across occurrences; only the class does.
			second.Metadata().PutStr(GenerationHintMetadataKey, class)
			second.Metadata().PutStr("metricsgen.hint.variation", "0.9")

			hints, err := ExtractGenerationHints(&metrics)
			require.NoError(t, err)
			assert.Equal(t, map[string]distribution.MetricGenerationHint{
				"test.metric": {Class: distribution.GenerationHintClass(class)},
			}, hints)
			assert.Equal(t, passThrough, first.Metadata().AsRaw())
			assert.Equal(t, map[string]any{"metricsgen.hint.variation": "0.9"}, second.Metadata().AsRaw())
		})
	}
	t.Run("no class", func(t *testing.T) {
		metrics := pmetric.NewMetrics()
		m := appendGaugeMetric(metrics, "test.metric")
		require.NoError(t, m.Metadata().FromRaw(passThrough))

		hints, err := ExtractGenerationHints(&metrics)
		require.NoError(t, err)
		assert.Empty(t, hints)
		assert.Equal(t, passThrough, m.Metadata().AsRaw())
	})
}

// paramClasses are the classes that take parameters, with the parameters each requires.
var paramClasses = []struct {
	class    string
	required map[string]any
}{
	{class: "bounded_gauge", required: map[string]any{}},
}

func TestExtractGenerationHintsParamClassesAreStrict(t *testing.T) {
	withHint := func(class string, required map[string]any, extra map[string]any) map[string]any {
		metadata := map[string]any{"metricsgen.hint.class": class}
		for _, m := range []map[string]any{required, extra} {
			for k, v := range m {
				metadata["metricsgen.hint."+k] = v
			}
		}
		return metadata
	}
	for _, pc := range paramClasses {
		require.True(t, distribution.GenerationHintClass(pc.class).TakesParams(), pc.class)
		tests := []struct {
			name      string
			metadata  []map[string]any
			wantError string
		}{
			{
				name:      "unknown parameter",
				metadata:  []map[string]any{withHint(pc.class, pc.required, map[string]any{"foo": "x"})},
				wantError: `metric "test.metric" with generation hint ` + pc.class + `: unknown parameter metricsgen.hint.foo`,
			},
			{
				name:      "non-string parameter",
				metadata:  []map[string]any{withHint(pc.class, pc.required, map[string]any{"variation": int64(1)})},
				wantError: `metric "test.metric" has non-string metricsgen.hint.variation metadata`,
			},
			{
				name: "different parameters across occurrences",
				metadata: []map[string]any{
					withHint(pc.class, pc.required, map[string]any{"variation": "0.1"}),
					withHint(pc.class, pc.required, map[string]any{"variation": "0.2"}),
				},
				wantError: `metric "test.metric" must declare the same hint parameters across all occurrences`,
			},
		}
		for _, tc := range tests {
			t.Run(pc.class+" "+tc.name, func(t *testing.T) {
				metrics := pmetric.NewMetrics()
				var first pmetric.Metric
				for i, metadata := range tc.metadata {
					m := appendGaugeMetric(metrics, "test.metric")
					require.NoError(t, m.Metadata().FromRaw(metadata))
					if i == 0 {
						first = m
					}
				}
				hints, err := ExtractGenerationHints(&metrics)
				require.EqualError(t, err, tc.wantError)
				assert.Nil(t, hints)
				assert.Equal(t, tc.metadata[0], first.Metadata().AsRaw())
			})
		}
		t.Run(pc.class+" strips every hint key", func(t *testing.T) {
			metrics := pmetric.NewMetrics()
			m := appendGaugeMetric(metrics, "test.metric")
			require.NoError(t, m.Metadata().FromRaw(withHint(pc.class, pc.required, map[string]any{"variation": "0.1"})))
			m.Metadata().PutStr("prometheus.type", "gauge")

			_, err := ExtractGenerationHints(&metrics)
			require.NoError(t, err)
			assert.Equal(t, map[string]any{"prometheus.type": "gauge"}, m.Metadata().AsRaw())
		})
	}
}

func appendGaugeMetric(metrics pmetric.Metrics, name string) pmetric.Metric {
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	m.SetEmptyGauge()
	return m
}

func appendHistogramMetric(metrics pmetric.Metrics, name string) pmetric.Metric {
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	m.SetEmptyHistogram()
	return m
}

func appendExponentialHistogramMetric(metrics pmetric.Metrics, name string) pmetric.Metric {
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	m.SetEmptyExponentialHistogram()
	return m
}

func appendSummaryMetric(metrics pmetric.Metrics, name string) pmetric.Metric {
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	m.SetEmptySummary()
	return m
}
