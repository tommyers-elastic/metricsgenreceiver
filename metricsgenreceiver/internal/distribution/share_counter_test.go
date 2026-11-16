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

// shareCounterTemplate returns a template with a cumulative monotonic CPU time per state seeded
// over one day: rates of about 0.1, 0.05, 0.85 and 0 per second (times 1000 for ints).
func shareCounterTemplate(t *testing.T, valueType pmetric.NumberDataPointValueType) pmetric.Metrics {
	t.Helper()
	template := pmetric.NewMetrics()
	m := template.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test.cpu.time")
	points := counterSetup(m)
	for _, member := range []struct {
		state string
		rate  float64
	}{{"user", 0.1}, {"system", 0.05}, {"idle", 0.85}, {"steal", 0}} {
		point := points.AppendEmpty()
		point.Attributes().PutStr("state", member.state)
		point.SetStartTimestamp(seedStart)
		point.SetTimestamp(seedEnd)
		switch {
		case valueType == pmetric.NumberDataPointValueTypeInt:
			point.SetIntValue(int64(math.Round(member.rate * 86400 * 1000)))
		case member.rate > 0:
			point.SetDoubleValue(member.rate*86400 + 0.25)
		default:
			point.SetDoubleValue(0)
		}
	}
	return template
}

var shareCounterHints = map[string]MetricGenerationHint{
	"test.cpu.time": {Class: GenerationHintShareCounter, Params: map[string]string{"share_by": "state", "variation": "0.5", "period": "5m"}},
}

// emitCounterGroup returns the values of the four members of the share counter template for the
// instance at t with start timestamp t0, emitted with one scratch.
func emitCounterGroup(t *testing.T, plan *ModelPlan, template pmetric.Metrics, instanceID int, t0, at time.Time) []float64 {
	t.Helper()
	scratch := plan.GetScratch()
	defer plan.PutScratch(scratch)
	points := template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	values := make([]float64, points.Len())
	for i := range values {
		point := pmetric.NewNumberDataPoint()
		points.At(i).CopyTo(point)
		point.SetStartTimestamp(pcommon.NewTimestampFromTime(t0))
		point.SetTimestamp(pcommon.NewTimestampFromTime(at))
		require.True(t, plan.Handles(i))
		plan.Apply(point, i, instanceID, scratch)
		values[i] = currentNumberDataPointValue(point)
	}
	return values
}

func TestShareCounterAddsUpToTotalRate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType pmetric.NumberDataPointValueType
		rate      float64
		tolerance float64
	}{
		// Rounded per member to whole numbers: the sum is within 4 * 0.5 of R * elapsed.
		{name: "int", valueType: pmetric.NumberDataPointValueTypeInt, rate: 1000, tolerance: 2},
		// Rounded per member to 2 decimals.
		{name: "double", valueType: pmetric.NumberDataPointValueTypeDouble, rate: 1, tolerance: 0.02 + 1e-9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			template := shareCounterTemplate(t, tc.valueType)
			plan, err := PlanModels(&template, shareCounterHints, pmetric.AggregationTemporalityUnspecified)
			require.NoError(t, err)
			require.Len(t, plan.groups, 1)
			rate := plan.groups[0].total
			assert.InDelta(t, tc.rate, rate, 1e-4)
			firstDeltas := map[float64]struct{}{}
			for instance := 0; instance < 8; instance++ {
				previous := make([]float64, 4)
				deltas := map[float64]struct{}{}
				for i := 0; i <= 2000; i++ {
					at := modelTestStart.Add(time.Duration(i) * 10 * time.Second)
					values := emitCounterGroup(t, plan, template, instance, modelTestStart, at)
					sum := 0.0
					for k, value := range values {
						require.GreaterOrEqual(t, value, previous[k], "instance %d sample %d member %d", instance, i, k)
						sum += value
					}
					require.InDelta(t, rate*at.Sub(modelTestStart).Seconds(), sum, tc.tolerance, "instance %d sample %d", instance, i)
					require.Zero(t, values[3])
					if i > 0 {
						deltas[values[0]-previous[0]] = struct{}{}
					}
					if i == 1 {
						firstDeltas[values[0]] = struct{}{}
					}
					previous = values
				}
				// The rate varies over time.
				assert.Greater(t, len(deltas), 20, "instance %d", instance)
			}
			// And between instances.
			assert.Greater(t, len(firstDeltas), 4)
		})
	}
}

func TestShareCounterCountsFromTheStartTimestamp(t *testing.T) {
	template := shareCounterTemplate(t, pmetric.NumberDataPointValueTypeInt)
	plan, err := PlanModels(&template, shareCounterHints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	restart := modelTestStart.Add(7*time.Hour + 13*time.Second)
	for _, elapsed := range []time.Duration{0, time.Second, 25 * time.Second, time.Hour} {
		values := emitCounterGroup(t, plan, template, 3, restart, restart.Add(elapsed))
		sum := 0.0
		for _, value := range values {
			sum += value
		}
		assert.InDelta(t, 1000*elapsed.Seconds(), sum, 2, elapsed)
		if elapsed == 0 {
			assert.Equal(t, []float64{0, 0, 0, 0}, values)
		}
	}
	// Before the start timestamp (which the receiver never emits), the values are 0.
	assert.Equal(t, []float64{0, 0, 0, 0}, emitCounterGroup(t, plan, template, 3, restart, restart.Add(-time.Second)))
}

func TestShareCounterRateEqualsShareGaugeShare(t *testing.T) {
	counter := shareCounterTemplate(t, pmetric.NumberDataPointValueTypeDouble)
	gauge := modelTemplate("test.cpu.utilization", gaugeSetup)
	gaugePoints := gauge.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	counterPoints := counter.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	for i := 0; i < counterPoints.Len(); i++ {
		point := gaugePoints.AppendEmpty()
		counterPoints.At(i).Attributes().CopyTo(point.Attributes())
		point.SetDoubleValue(counterPoints.At(i).DoubleValue())
	}
	params := map[string]string{"share_by": "state", "variation_key": "cpu", "variation": "0.5", "period": "5m"}
	counterPlan, err := PlanModels(&counter, map[string]MetricGenerationHint{"test.cpu.time": {Class: GenerationHintShareCounter, Params: params}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	gaugePlan, err := PlanModels(&gauge, map[string]MetricGenerationHint{"test.cpu.utilization": {Class: GenerationHintShareGauge, Params: params}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)

	t0 := modelTestStart.UnixNano()
	const step = int64(time.Millisecond)
	before, after := make([]float64, 4), make([]float64, 4)
	shares := make([]float64, 4)
	work := make([]float64, 4)
	for instance := 0; instance < 4; instance++ {
		for i := 1; i < 200; i++ {
			at := t0 + int64(i)*int64(37*time.Second)
			counterPlan.integratedShares(&counterPlan.groups[0], instance, t0, at-step, before, work)
			counterPlan.integratedShares(&counterPlan.groups[0], instance, t0, at+step, after, work)
			gaugePlan.shares(&gaugePlan.groups[0], instance, at, shares, work)
			for k := range shares {
				rateShare := (after[k] - before[k]) / (2 * float64(step) / float64(time.Second))
				require.InDelta(t, shares[k], rateShare, 1e-6, "instance %d sample %d member %d", instance, i, k)
			}
		}
	}
}

func TestNoiseIntegral(t *testing.T) {
	n := newNoise(7, 2, 10*time.Minute)
	t0 := modelTestStart.UnixNano()
	// The closed form matches a numerical integration.
	numeric := 0.0
	const step = int64(100 * time.Millisecond)
	for at := t0; at < t0+int64(time.Hour); at += step {
		numeric += (n.value(at) + n.value(at+step)) / 2 * (float64(step) / float64(time.Second))
	}
	assert.InDelta(t, numeric, n.integral(t0, t0+int64(time.Hour)), 1e-3)
	assert.Zero(t, n.integral(t0, t0))
	// Integrals over adjacent intervals add up.
	middle := t0 + int64(17*time.Minute)
	assert.InDelta(t, n.integral(t0, t0+int64(time.Hour)), n.integral(t0, middle)+n.integral(middle, t0+int64(time.Hour)), 1e-9)
}

func TestShareCounterStartupErrors(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(pmetric.Metric)
		temporality pmetric.AggregationTemporality
		wantError   string
	}{
		{
			name:      "gauge",
			modify:    func(m pmetric.Metric) { m.Sum().DataPoints().CopyTo(m.SetEmptyGauge().DataPoints()) },
			wantError: `metric "test.cpu.time" with generation hint share_counter: the metric must be a cumulative monotonic sum`,
		},
		{
			name:      "non-monotonic sum",
			modify:    func(m pmetric.Metric) { m.Sum().SetIsMonotonic(false) },
			wantError: `metric "test.cpu.time" with generation hint share_counter: the metric must be a cumulative monotonic sum`,
		},
		{
			name:      "delta sum",
			modify:    func(m pmetric.Metric) { m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta) },
			wantError: `metric "test.cpu.time" with generation hint share_counter: the metric must be a cumulative monotonic sum`,
		},
		{
			name:        "delta temporality override",
			temporality: pmetric.AggregationTemporalityDelta,
			wantError:   `metric "test.cpu.time" with generation hint share_counter: the metric must be a cumulative monotonic sum, but temporality_override makes it delta`,
		},
		{
			name:      "no start timestamp",
			modify:    func(m pmetric.Metric) { m.Sum().DataPoints().At(1).SetStartTimestamp(0) },
			wantError: `metric "test.cpu.time" with generation hint share_counter: the data point with attributes {state=system}: the data point needs both startTimeUnixNano and timeUnixNano`,
		},
		{
			name:      "negative value",
			modify:    func(m pmetric.Metric) { m.Sum().DataPoints().At(0).SetDoubleValue(-1) },
			wantError: `metric "test.cpu.time" with generation hint share_counter: the data point with attributes {state=user}: the value (-1) must be a finite number that is not negative`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			template := shareCounterTemplate(t, pmetric.NumberDataPointValueTypeDouble)
			if tc.modify != nil {
				tc.modify(template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0))
			}
			_, err := PlanModels(&template, shareCounterHints, tc.temporality)
			require.EqualError(t, err, tc.wantError)
		})
	}
	// A cumulative override is fine.
	template := shareCounterTemplate(t, pmetric.NumberDataPointValueTypeDouble)
	_, err := PlanModels(&template, shareCounterHints, pmetric.AggregationTemporalityCumulative)
	require.NoError(t, err)
	// share_counter takes no total.
	err = MetricGenerationHint{Class: GenerationHintShareCounter, Params: map[string]string{"share_by": "state", "total": "1"}}.Validate("test.cpu.time")
	require.EqualError(t, err, `metric "test.cpu.time" with generation hint share_counter: unknown parameter metricsgen.hint.total`)
}

func TestShareCounterMonotonicOverLongRuns(t *testing.T) {
	// A member with a tiny share of a fast counter, sampled over a long time with jitter-like
	// timestamps, never decreases after rounding.
	template := shareCounterTemplate(t, pmetric.NumberDataPointValueTypeInt)
	template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(1).SetIntValue(3)
	plan, err := PlanModels(&template, shareCounterHints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	previous := make([]float64, 4)
	for i := 0; i < 20000; i++ {
		at := modelTestStart.Add(time.Duration(i)*time.Minute + time.Duration(i%7)*time.Second)
		values := emitCounterGroup(t, plan, template, 1, modelTestStart, at)
		for k := range values {
			require.GreaterOrEqual(t, values[k], previous[k], "sample %d member %d", i, k)
			require.False(t, math.IsNaN(values[k]))
		}
		previous = values
	}
	assert.Positive(t, previous[1])
}

// counterGroupTemplate returns a template with a share_counter metric "test.counter" with one
// member per seed, counted over one day, as ints or doubles.
func counterGroupTemplate(ints bool, seeds ...float64) pmetric.Metrics {
	template := pmetric.NewMetrics()
	m := template.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test.counter")
	points := counterSetup(m)
	for i, seed := range seeds {
		point := points.AppendEmpty()
		point.Attributes().PutStr("state", string(rune('a'+i)))
		point.SetStartTimestamp(seedStart)
		point.SetTimestamp(seedEnd)
		if ints {
			point.SetIntValue(int64(seed))
		} else {
			point.SetDoubleValue(seed)
		}
	}
	return template
}

func TestShareCounterMonotonicAtFullVariation(t *testing.T) {
	// Members with a tiny share of a fast group, variation 1 and short periods, sampled over
	// years since their start: their rates never fall below 1/1000 of their mean rate, so they
	// never decrease.
	year := 365 * 24 * time.Hour
	for _, tc := range []struct {
		name   string
		ints   bool
		seeds  []float64
		params map[string]string
	}{
		{name: "double tiny share", seeds: []float64{1e-6, 8.64e9 + 0.5}, params: map[string]string{"period": "10s"}},
		{name: "double tiny shares wide spread", seeds: []float64{1e-4, 0.0001, 8.64e12 + 0.25}, params: map[string]string{"period": "1h", "instance_spread": "1000"}},
		{name: "int", ints: true, seeds: []float64{1, 8.64e12}, params: map[string]string{"period": "37s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			template := counterGroupTemplate(tc.ints, tc.seeds...)
			params := map[string]string{"share_by": "state", "variation": "1"}
			for k, v := range tc.params {
				params[k] = v
			}
			hints := map[string]MetricGenerationHint{"test.counter": {Class: GenerationHintShareCounter, Params: params}}
			plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
			require.NoError(t, err)
			g := &plan.groups[0]
			for _, instance := range []int{0, 1, 2} {
				for _, offset := range []time.Duration{0, year, 3 * year} {
					var previous []float64
					for i := 0; i < 5000; i++ {
						at := modelTestStart.Add(offset + time.Duration(i)*10*time.Second + time.Duration(i%7)*1234567)
						values := emitCounterGroup(t, plan, template, instance, modelTestStart, at)
						for k := range values {
							require.False(t, previous != nil && values[k] < previous[k], "instance %d offset %v sample %d member %d: %v after %v", instance, offset, i, k, values[k], previous)
						}
						previous = values
					}
				}
				// The rate of the smallest member over any 10s interval is at least 1/1000 of its
				// mean rate.
				integrals := make([]float64, len(g.members))
				later := make([]float64, len(g.members))
				work := make([]float64, len(g.members))
				f := meanShare(plan, 0, g.members[0], instance)
				for i := 0; i < 2000; i++ {
					at := modelTestStart.Add(time.Duration(i) * 7 * time.Second).UnixNano()
					plan.integratedShares(g, instance, modelTestStart.UnixNano(), at, integrals, work)
					plan.integratedShares(g, instance, modelTestStart.UnixNano(), at+int64(10*time.Second), later, work)
					require.Greater(t, later[0]-integrals[0], 10*f/1000*(1-1e-6), "instance %d sample %d", instance, i)
				}
			}
		})
	}
}

func TestShareCounterIntValuesStayInTheInt64Range(t *testing.T) {
	// A group counting 2^53 per day exceeds the int64 range in its members after about 6 years;
	// they stop at the top of the range, on every architecture, and stay monotonic.
	template := counterGroupTemplate(true, 1<<52, 1<<52)
	hints := map[string]MetricGenerationHint{"test.counter": {Class: GenerationHintShareCounter, Params: map[string]string{"share_by": "state"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	previous := []float64{0, 0}
	for years := 2; years <= 200; years += 2 {
		at := time.Unix(0, 0).Add(time.Duration(years) * 365 * 24 * time.Hour)
		points := template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
		scratch := plan.GetScratch()
		for i := 0; i < points.Len(); i++ {
			point := pmetric.NewNumberDataPoint()
			points.At(i).CopyTo(point)
			point.SetStartTimestamp(pcommon.Timestamp(0))
			point.SetTimestamp(pcommon.NewTimestampFromTime(at))
			plan.Apply(point, i, 4, scratch)
			value := point.IntValue()
			require.GreaterOrEqual(t, float64(value), previous[i], "year %d member %d", years, i)
			previous[i] = float64(value)
		}
		plan.PutScratch(scratch)
	}
	assert.Equal(t, float64(math.MaxInt64), previous[0])
	assert.Equal(t, float64(math.MaxInt64), previous[1])
}

func TestShareCounterIntSeedsUpTo2To53(t *testing.T) {
	template := counterGroupTemplate(true, 1<<53, 0)
	template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(1).SetIntValue(1)
	hints := map[string]MetricGenerationHint{"test.counter": {Class: GenerationHintShareCounter, Params: map[string]string{"share_by": "state"}}}
	_, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.EqualError(t, err, `metric "test.counter" with generation hint share_counter: the int share group with attributes {} adds up to more than 2^53 (9007199254740992), the largest total up to which int values add up exactly`)
	template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(1).SetIntValue(0)
	_, err = PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
}

func TestShareCounterVariationKeyParameters(t *testing.T) {
	// A share_gauge utilization only has the counter's rate shares with the same parameters.
	template := shareCounterTemplate(t, pmetric.NumberDataPointValueTypeDouble)
	utilization := appendMetric(template)
	utilization.SetName("test.cpu.utilization")
	template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().CopyTo(utilization.SetEmptyGauge().DataPoints())
	counter := map[string]string{"share_by": "state", "variation_key": "cpu", "variation": "0.5"}
	for _, tc := range []struct {
		gauge     map[string]string
		wantError string
	}{
		{gauge: map[string]string{"share_by": "state", "variation_key": "cpu", "variation": "0.5", "total": "1"}},
		{
			gauge:     map[string]string{"share_by": "state", "variation_key": "cpu", "total": "1"},
			wantError: `metrics "test.cpu.time" and "test.cpu.utilization" have the same variation key "cpu" (metricsgen.hint.variation_key, or the metric name) but different metricsgen.hint.variation ("0.5" and "0.25")`,
		},
		{
			gauge:     map[string]string{"share_by": "state", "variation_key": "cpu", "variation": "0.5", "period": "1m"},
			wantError: `metrics "test.cpu.time" and "test.cpu.utilization" have the same variation key "cpu" (metricsgen.hint.variation_key, or the metric name) but different metricsgen.hint.period ("10m0s" and "1m0s")`,
		},
	} {
		hints := map[string]MetricGenerationHint{
			"test.cpu.time":        {Class: GenerationHintShareCounter, Params: counter},
			"test.cpu.utilization": {Class: GenerationHintShareGauge, Params: tc.gauge},
		}
		_, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
		if tc.wantError == "" {
			require.NoError(t, err)
			continue
		}
		require.ErrorContains(t, err, tc.wantError)
	}
}

func TestShareGroupScratchIsAllocationFree(t *testing.T) {
	template := shareCounterTemplate(t, pmetric.NumberDataPointValueTypeInt)
	plan, err := PlanModels(&template, shareCounterHints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	scratch := plan.newScratch()
	t0 := modelTestStart.UnixNano()
	tick := t0
	sum := 0.0
	allocs := testing.AllocsPerRun(100, func() {
		// A fresh emission, as from GetScratch.
		clear(scratch.ready)
		tick += int64(10 * time.Second)
		for i := range plan.series {
			sum += plan.memberValue(&plan.series[i], 3, t0, tick, scratch)
		}
	})
	assert.Zero(t, allocs)
	assert.Positive(t, sum)
	// A fresh scratch per emission computes the group again for the new tick.
	at := time.Unix(0, tick)
	assert.NotEqual(t, emitCounterGroup(t, plan, template, 3, modelTestStart, at), emitCounterGroup(t, plan, template, 3, modelTestStart, at.Add(time.Minute)))
}
