package distribution

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var modelTestStart = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func TestHintParamsValidation(t *testing.T) {
	tests := []struct {
		name      string
		class     GenerationHintClass
		params    map[string]string
		wantError string
	}{
		{name: "defaults", class: GenerationHintBoundedGauge},
		{name: "all parameters", class: GenerationHintBoundedGauge, params: map[string]string{"variation": "0.5", "period": "1h", "instance_spread": "1", "min": "-1", "max": "1", "variation_key": "load"}},
		{name: "min equals max", class: GenerationHintBoundedGauge, params: map[string]string{"min": "2", "max": "2"}},
		{name: "existing class takes no parameters", class: GenerationHintSlowGauge, params: map[string]string{"variation": "0.5"}, wantError: `metric "test.metric" with generation hint slow_gauge: unknown parameter metricsgen.hint.variation`},
		{name: "unknown parameter", class: GenerationHintBoundedGauge, params: map[string]string{"varaition": "0.5"}, wantError: `metric "test.metric" with generation hint bounded_gauge: unknown parameter metricsgen.hint.varaition`},
		{name: "variation not a number", class: GenerationHintBoundedGauge, params: map[string]string{"variation": "much"}, wantError: `metric "test.metric" with generation hint bounded_gauge: parameter metricsgen.hint.variation must be a number in [0, 1], got "much"`},
		{name: "variation above 1", class: GenerationHintBoundedGauge, params: map[string]string{"variation": "1.5"}, wantError: `parameter metricsgen.hint.variation must be a number in [0, 1], got "1.5"`},
		{name: "variation negative", class: GenerationHintBoundedGauge, params: map[string]string{"variation": "-0.1"}, wantError: `parameter metricsgen.hint.variation must be a number in [0, 1], got "-0.1"`},
		{name: "variation NaN", class: GenerationHintBoundedGauge, params: map[string]string{"variation": "NaN"}, wantError: `parameter metricsgen.hint.variation must be a number in [0, 1], got "NaN"`},
		{name: "period zero", class: GenerationHintBoundedGauge, params: map[string]string{"period": "0s"}, wantError: `parameter metricsgen.hint.period must be a positive duration of at most 8760h0m0s, got "0s"`},
		{name: "period negative", class: GenerationHintBoundedGauge, params: map[string]string{"period": "-1m"}, wantError: `parameter metricsgen.hint.period must be a positive duration`},
		{name: "period not a duration", class: GenerationHintBoundedGauge, params: map[string]string{"period": "10"}, wantError: `parameter metricsgen.hint.period must be a positive duration`},
		{name: "period too long", class: GenerationHintBoundedGauge, params: map[string]string{"period": "8761h"}, wantError: `parameter metricsgen.hint.period must be a positive duration`},
		{name: "instance spread below 1", class: GenerationHintBoundedGauge, params: map[string]string{"instance_spread": "0.5"}, wantError: `parameter metricsgen.hint.instance_spread must be a number >= 1, got "0.5"`},
		{name: "min not a number", class: GenerationHintBoundedGauge, params: map[string]string{"min": "low"}, wantError: `parameter metricsgen.hint.min must be a number, got "low"`},
		{name: "max infinite", class: GenerationHintBoundedGauge, params: map[string]string{"max": "+Inf"}, wantError: `parameter metricsgen.hint.max must be a number, got "+Inf"`},
		{name: "min above max", class: GenerationHintBoundedGauge, params: map[string]string{"min": "2", "max": "1"}, wantError: `metric "test.metric" with generation hint bounded_gauge: parameter metricsgen.hint.min (2) must not be greater than metricsgen.hint.max (1)`},
		{name: "empty variation key", class: GenerationHintBoundedGauge, params: map[string]string{"variation_key": ""}, wantError: `parameter metricsgen.hint.variation_key must be a non-empty string, got ""`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := MetricGenerationHint{Class: tc.class, Params: tc.params}.Validate("test.metric")
			if tc.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantError)
			assert.Contains(t, err.Error(), `"test.metric"`)
		})
	}
}

func TestHintParamsDefaults(t *testing.T) {
	params, err := MetricGenerationHint{Class: GenerationHintBoundedGauge}.parseParams("test.metric")
	require.NoError(t, err)
	assert.Equal(t, modelParams{variation: 0.25, period: 10 * time.Minute, instanceSpread: 2}, params)
}

// modelTemplate returns a template with one metric per entry of values, with one data point per
// value, attributed with state=<index>.
func modelTemplate(name string, setup func(pmetric.Metric) pmetric.NumberDataPointSlice, values ...float64) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	points := setup(m)
	for i, value := range values {
		point := points.AppendEmpty()
		point.Attributes().PutStr("state", string(rune('a'+i)))
		if value == math.Trunc(value) && value > 1 {
			point.SetIntValue(int64(value))
		} else {
			point.SetDoubleValue(value)
		}
	}
	return metrics
}

func gaugeSetup(m pmetric.Metric) pmetric.NumberDataPointSlice {
	return m.SetEmptyGauge().DataPoints()
}

func upDownSumSetup(m pmetric.Metric) pmetric.NumberDataPointSlice {
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(false)
	return sum.DataPoints()
}

func counterSetup(m pmetric.Metric) pmetric.NumberDataPointSlice {
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	return sum.DataPoints()
}

// emitModel returns the value the plan computes for the template data point at idx for the
// instance at time at.
func emitModel(t *testing.T, plan *ModelPlan, template pmetric.Metrics, idx int, instanceID int, at time.Time) float64 {
	t.Helper()
	point := pmetric.NewNumberDataPoint()
	templatePoint(template, idx).CopyTo(point)
	point.SetStartTimestamp(pcommon.NewTimestampFromTime(modelTestStart))
	point.SetTimestamp(pcommon.NewTimestampFromTime(at))
	require.True(t, plan.Handles(idx))
	plan.Apply(point, idx, instanceID)
	return currentNumberDataPointValue(point)
}

func templatePoint(template pmetric.Metrics, idx int) pmetric.NumberDataPoint {
	var found pmetric.NumberDataPoint
	i := 0
	ms := template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for k := 0; k < ms.Len(); k++ {
		m := ms.At(k)
		var points pmetric.NumberDataPointSlice
		if m.Type() == pmetric.MetricTypeGauge {
			points = m.Gauge().DataPoints()
		} else {
			points = m.Sum().DataPoints()
		}
		for l := 0; l < points.Len(); l++ {
			if i == idx {
				found = points.At(l)
			}
			i++
		}
	}
	return found
}

func TestPlanModelsNoModelHints(t *testing.T) {
	template := modelTemplate("test.gauge", gaugeSetup, 1.5)
	plan, err := PlanModels(&template, map[string]MetricGenerationHint{"test.gauge": {Class: GenerationHintSlowGauge}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	assert.Nil(t, plan)
	assert.False(t, plan.Handles(0))
}

func TestBoundedGaugeRejectsCounters(t *testing.T) {
	template := modelTemplate("test.counter", counterSetup, 1.5)
	_, err := PlanModels(&template, map[string]MetricGenerationHint{"test.counter": {Class: GenerationHintBoundedGauge}}, pmetric.AggregationTemporalityUnspecified)
	require.EqualError(t, err, `metric "test.counter" with generation hint bounded_gauge: the metric must be a gauge or a non-monotonic sum`)
}

func TestBoundedGaugeStaysAroundItsCentre(t *testing.T) {
	const seed = 1.5
	for _, tc := range []struct {
		name      string
		params    map[string]string
		variation float64
		spread    float64
	}{
		{name: "defaults", variation: 0.25, spread: 2},
		{name: "wide", params: map[string]string{"variation": "1", "instance_spread": "4", "period": "1m"}, variation: 1, spread: 4},
		{name: "no instance spread", params: map[string]string{"instance_spread": "1"}, variation: 0.25, spread: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			template := modelTemplate("test.gauge", gaugeSetup, seed)
			hints := map[string]MetricGenerationHint{"test.gauge": {Class: GenerationHintBoundedGauge, Params: tc.params}}
			plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
			require.NoError(t, err)
			s := plan.series[0]
			for instance := 0; instance < 20; instance++ {
				k := s.instanceFactor(instance)
				assert.GreaterOrEqual(t, k, 1/tc.spread-1e-12)
				assert.LessOrEqual(t, k, tc.spread+1e-12)
				centre := seed * k
				minSeen, maxSeen := math.Inf(1), math.Inf(-1)
				sum := 0.0
				const samples = 6 * 24 * 7 // one week every 10 minutes and 7 seconds
				for i := 0; i < samples; i++ {
					value := emitModel(t, plan, template, 0, instance, modelTestStart.Add(time.Duration(i)*(10*time.Minute+7*time.Second)))
					// Mean reversion: never further from the centre than variation (plus rounding).
					require.LessOrEqual(t, math.Abs(value-centre), tc.variation*centre+0.05, "instance %d sample %d", instance, i)
					minSeen, maxSeen = math.Min(minSeen, value), math.Max(maxSeen, value)
					sum += value
				}
				// It moves, and it keeps coming back: the long-run mean is near the centre.
				assert.Greater(t, maxSeen-minSeen, 0.2*tc.variation*centre, "instance %d", instance)
				assert.InDelta(t, centre, sum/samples, 0.25*tc.variation*centre+0.05, "instance %d", instance)
			}
		})
	}
}

func TestBoundedGaugeClampAndInts(t *testing.T) {
	template := modelTemplate("test.usage", upDownSumSetup, 4000000000)
	hints := map[string]MetricGenerationHint{"test.usage": {Class: GenerationHintBoundedGauge, Params: map[string]string{
		"variation": "1", "min": "3000000000.5", "max": "5000000000.5",
	}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	clampedLow, clampedHigh := false, false
	for instance := 0; instance < 10; instance++ {
		for i := 0; i < 500; i++ {
			point := pmetric.NewNumberDataPoint()
			templatePoint(template, 0).CopyTo(point)
			point.SetTimestamp(pcommon.NewTimestampFromTime(modelTestStart.Add(time.Duration(i) * 37 * time.Second)))
			plan.Apply(point, 0, instance)
			require.Equal(t, pmetric.NumberDataPointValueTypeInt, point.ValueType())
			value := point.IntValue()
			require.GreaterOrEqual(t, value, int64(3000000001))
			require.LessOrEqual(t, value, int64(5000000000))
			clampedLow = clampedLow || value == 3000000001
			clampedHigh = clampedHigh || value == 5000000000
		}
	}
	assert.True(t, clampedLow)
	assert.True(t, clampedHigh)
}

func TestBoundedGaugeIsStateless(t *testing.T) {
	template := modelTemplate("test.gauge", gaugeSetup, 1.5, 0.75, 0)
	hints := map[string]MetricGenerationHint{"test.gauge": {Class: GenerationHintBoundedGauge}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	at := modelTestStart.Add(3 * time.Hour)
	first := emitModel(t, plan, template, 1, 7, at)
	// Other instances, series and times in between do not change the value.
	for instance := 0; instance < 5; instance++ {
		for idx := 0; idx < 3; idx++ {
			emitModel(t, plan, template, idx, instance, at.Add(time.Duration(instance)*time.Minute))
		}
	}
	assert.Equal(t, first, emitModel(t, plan, template, 1, 7, at))
	// Instances differ; a zero seed stays zero.
	assert.NotEqual(t, first, emitModel(t, plan, template, 1, 8, at))
	assert.Zero(t, emitModel(t, plan, template, 2, 7, at))

	// The value does not depend on the plan either: a second plan computes the same.
	again, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	assert.Equal(t, first, emitModel(t, again, template, 1, 7, at))
}

func TestBoundedGaugeVariationKey(t *testing.T) {
	template := pmetric.NewMetrics()
	ms := template.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for _, name := range []string{"test.a", "test.b", "test.c"} {
		m := ms.AppendEmpty()
		m.SetName(name)
		point := m.SetEmptyGauge().DataPoints().AppendEmpty()
		point.Attributes().PutStr("device", "sda")
		point.SetDoubleValue(2)
	}
	keyed := map[string]string{"variation_key": "disk"}
	hints := map[string]MetricGenerationHint{
		"test.a": {Class: GenerationHintBoundedGauge, Params: keyed},
		"test.b": {Class: GenerationHintBoundedGauge, Params: keyed},
		"test.c": {Class: GenerationHintBoundedGauge},
	}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	differs := false
	for i := 0; i < 50; i++ {
		at := modelTestStart.Add(time.Duration(i) * time.Minute)
		a := emitModel(t, plan, template, 0, 3, at)
		assert.Equal(t, a, emitModel(t, plan, template, 1, 3, at))
		differs = differs || a != emitModel(t, plan, template, 2, 3, at)
	}
	assert.True(t, differs)
}

func TestNoise(t *testing.T) {
	n := newNoise(42, 3, 10*time.Minute)
	sum := 0.0
	const samples = 100000
	for i := 0; i < samples; i++ {
		v := n.value(modelTestStart.Add(time.Duration(i) * 7 * time.Second).UnixNano())
		require.GreaterOrEqual(t, v, -1.0)
		require.LessOrEqual(t, v, 1.0)
		sum += v
	}
	assert.InDelta(t, 0, sum/samples, 0.02)
	for i, c := range n {
		base := float64(10*time.Minute) * noisePeriodScales[i]
		assert.GreaterOrEqual(t, float64(c.period), 0.75*base-1)
		assert.Less(t, float64(c.period), 1.25*base)
	}
	// Times before 1970 work too.
	v := n.value(time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	assert.GreaterOrEqual(t, v, -1.0)
	assert.LessOrEqual(t, v, 1.0)
}

func TestBoundedGaugeVariationKeyParameters(t *testing.T) {
	template := pmetric.NewMetrics()
	ms := template.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for _, name := range []string{"test.a", "test.b"} {
		m := ms.AppendEmpty()
		m.SetName(name)
		m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(2.5)
	}
	tests := []struct {
		name      string
		a, b      map[string]string
		wantError string
	}{
		{name: "same parameters", a: map[string]string{"variation_key": "k"}, b: map[string]string{"variation_key": "k"}},
		{name: "different variation", a: map[string]string{"variation_key": "k"}, b: map[string]string{"variation_key": "k", "variation": "0.5", "min": "0"}},
		{name: "different keys", a: map[string]string{"variation_key": "k"}, b: map[string]string{"variation_key": "l", "period": "1h"}},
		{
			name:      "different period",
			a:         map[string]string{"variation_key": "k"},
			b:         map[string]string{"variation_key": "k", "period": "1h"},
			wantError: `metrics "test.a" and "test.b" have the same variation key "k" (metricsgen.hint.variation_key, or the metric name) but different metricsgen.hint.period ("10m0s" and "1h0m0s"): their series only vary together when it is the same`,
		},
		{
			name:      "different instance spread",
			a:         map[string]string{"variation_key": "k", "instance_spread": "3"},
			b:         map[string]string{"variation_key": "k"},
			wantError: `metrics "test.a" and "test.b" have the same variation key "k" (metricsgen.hint.variation_key, or the metric name) but different metricsgen.hint.instance_spread ("3" and "2")`,
		},
		{
			name:      "key equal to the other metric's name",
			a:         nil,
			b:         map[string]string{"variation_key": "test.a", "period": "1h"},
			wantError: `metrics "test.a" and "test.b" have the same variation key "test.a" (metricsgen.hint.variation_key, or the metric name) but different metricsgen.hint.period ("10m0s" and "1h0m0s")`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hints := map[string]MetricGenerationHint{
				"test.a": {Class: GenerationHintBoundedGauge, Params: tc.a},
				"test.b": {Class: GenerationHintBoundedGauge, Params: tc.b},
			}
			_, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
			if tc.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantError)
		})
	}
}

func TestBoundedGaugeIntBoundsWithoutWholeNumber(t *testing.T) {
	bounds := map[string]string{"min": "0.2", "max": "0.8"}
	template := modelTemplate("test.gauge", gaugeSetup, 5)
	_, err := PlanModels(&template, map[string]MetricGenerationHint{"test.gauge": {Class: GenerationHintBoundedGauge, Params: bounds}}, pmetric.AggregationTemporalityUnspecified)
	require.EqualError(t, err, `metric "test.gauge" with generation hint bounded_gauge: the metric has int values but there is no whole number between metricsgen.hint.min (0.2) and metricsgen.hint.max (0.8)`)

	// Double values may stay between them, and int values need one whole number in between.
	doubles := modelTemplate("test.gauge", gaugeSetup, 0.5)
	_, err = PlanModels(&doubles, map[string]MetricGenerationHint{"test.gauge": {Class: GenerationHintBoundedGauge, Params: bounds}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	plan, err := PlanModels(&template, map[string]MetricGenerationHint{"test.gauge": {Class: GenerationHintBoundedGauge, Params: map[string]string{"min": "0.2", "max": "1"}}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	assert.Equal(t, 1.0, emitModel(t, plan, template, 0, 3, modelTestStart))
}

func TestBoundedGaugeIntValuesStayInTheInt64Range(t *testing.T) {
	// Seeds near the top of the int64 range times an instance factor of up to 8 leave the range;
	// they are clamped to it on every architecture instead of wrapping on some.
	template := modelTemplate("test.gauge", gaugeSetup, 4e18)
	hints := map[string]MetricGenerationHint{"test.gauge": {Class: GenerationHintBoundedGauge, Params: map[string]string{"instance_spread": "8", "variation": "1"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	require.Equal(t, pmetric.NumberDataPointValueTypeInt, templatePoint(template, 0).ValueType())
	clamped := false
	for instance := 0; instance < 50; instance++ {
		value := emitModel(t, plan, template, 0, instance, modelTestStart)
		require.GreaterOrEqual(t, value, 0.0, "instance %d", instance)
		clamped = clamped || value == float64(math.MaxInt64)
	}
	assert.True(t, clamped)

	assert.Equal(t, int64(math.MaxInt64), modelIntValue(1e19))
	assert.Equal(t, int64(math.MaxInt64), modelIntValue(math.Inf(1)))
	assert.Equal(t, int64(math.MinInt64), modelIntValue(-1e19))
	assert.Equal(t, int64(math.MinInt64), modelIntValue(math.Inf(-1)))
	assert.Equal(t, int64(0), modelIntValue(math.NaN()))
	assert.Equal(t, int64(-42), modelIntValue(-42))
	assert.Equal(t, int64(1<<62), modelIntValue(1<<62))
}
