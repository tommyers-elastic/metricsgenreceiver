package distribution

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var (
	seedStart   = pcommon.NewTimestampFromTime(time.Date(2024, 12, 16, 0, 0, 0, 0, time.UTC))
	seedEnd     = pcommon.NewTimestampFromTime(time.Date(2024, 12, 17, 0, 0, 0, 0, time.UTC))
	seededHints = map[string]MetricGenerationHint{
		"test.seeded": {Class: GenerationHintSeededRateCounter},
	}
)

func TestSeededRateCounterHintIsValid(t *testing.T) {
	require.NoError(t, MetricGenerationHint{Class: GenerationHintSeededRateCounter}.Validate("test.seeded"))
}

func TestSeededRates(t *testing.T) {
	metrics := pmetric.NewMetrics()
	ms := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	seeded := ms.AppendEmpty()
	seeded.SetName("test.seeded")
	points := seeded.SetEmptySum().DataPoints()
	for _, value := range []float64{864000, 43.2, 0} {
		point := points.AppendEmpty()
		point.SetStartTimestamp(seedStart)
		point.SetTimestamp(seedEnd)
		point.SetDoubleValue(value)
	}
	intPoint := points.AppendEmpty()
	intPoint.SetStartTimestamp(seedStart)
	intPoint.SetTimestamp(seedEnd)
	intPoint.SetIntValue(2592)
	other := ms.AppendEmpty()
	other.SetName("test.other")
	other.SetEmptySum().DataPoints().AppendEmpty().SetIntValue(5)

	rates, err := SeededRates(&metrics, seededHints)
	require.NoError(t, err)
	assert.Equal(t, []float64{10, 0.0005, 0, 0.03, 0}, rates)

	rates, err = SeededRates(&metrics, map[string]MetricGenerationHint{"test.seeded": {Class: GenerationHintSteadyCounter}})
	require.NoError(t, err)
	assert.Nil(t, rates, "no metric uses the hint")
}

func TestSeededRatesErrors(t *testing.T) {
	tests := []struct {
		name      string
		start     pcommon.Timestamp
		end       pcommon.Timestamp
		value     float64
		wantError string
	}{
		{name: "no start timestamp", end: seedEnd, value: 1, wantError: `metric "test.seeded" with generation hint seeded_rate_counter: the data point needs both startTimeUnixNano and timeUnixNano`},
		{name: "no timestamp", start: seedStart, value: 1, wantError: `metric "test.seeded" with generation hint seeded_rate_counter: the data point needs both startTimeUnixNano and timeUnixNano`},
		{name: "timestamp at start", start: seedStart, end: seedStart, value: 1, wantError: `metric "test.seeded" with generation hint seeded_rate_counter: timeUnixNano (1734307200000000000) must be after startTimeUnixNano (1734307200000000000)`},
		{name: "timestamp before start", start: seedEnd, end: seedStart, value: 1, wantError: `metric "test.seeded" with generation hint seeded_rate_counter: timeUnixNano (1734307200000000000) must be after startTimeUnixNano (1734393600000000000)`},
		{name: "negative value", start: seedStart, end: seedEnd, value: -1, wantError: `metric "test.seeded" with generation hint seeded_rate_counter: the value (-1) must be a finite number that is not negative`},
		{name: "NaN value", start: seedStart, end: seedEnd, value: math.NaN(), wantError: `metric "test.seeded" with generation hint seeded_rate_counter: the value (NaN) must be a finite number that is not negative`},
		{name: "infinite value", start: seedStart, end: seedEnd, value: math.Inf(1), wantError: `metric "test.seeded" with generation hint seeded_rate_counter: the value (+Inf) must be a finite number that is not negative`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metrics := pmetric.NewMetrics()
			m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("test.seeded")
			point := m.SetEmptySum().DataPoints().AppendEmpty()
			point.SetStartTimestamp(tc.start)
			point.SetTimestamp(tc.end)
			point.SetDoubleValue(tc.value)
			rates, err := SeededRates(&metrics, seededHints)
			require.EqualError(t, err, tc.wantError)
			assert.Nil(t, rates)
		})
	}
}

func TestSeededRatesRejectsGauges(t *testing.T) {
	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test.seeded")
	point := m.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetStartTimestamp(seedStart)
	point.SetTimestamp(seedEnd)
	point.SetDoubleValue(1)
	rates, err := SeededRates(&metrics, seededHints)
	require.EqualError(t, err, `metric "test.seeded" with generation hint seeded_rate_counter: the metric must be a sum`)
	assert.Nil(t, rates)
}

func TestSeededRateCounterKeepsTheRate(t *testing.T) {
	const intervals = 20000
	interval := 10 * time.Second
	tests := []struct {
		name      string
		valueType pmetric.NumberDataPointValueType
		precision map[string]int
		seed      float64
		rate      float64
	}{
		{name: "fast int", valueType: pmetric.NumberDataPointValueTypeInt, seed: 8640000000, rate: 100000},
		{name: "slow int below one unit", valueType: pmetric.NumberDataPointValueTypeInt, seed: 2592, rate: 0.03},
		{name: "fast double", valueType: pmetric.NumberDataPointValueTypeDouble, precision: map[string]int{"test.seeded": 2}, seed: 691200.25, rate: 8},
		{name: "slow double below one quantum", valueType: pmetric.NumberDataPointValueTypeDouble, precision: map[string]int{"test.seeded": 2}, seed: 34.56, rate: 0.0004},
		{name: "double without precision", valueType: pmetric.NumberDataPointValueTypeDouble, seed: 1, rate: 0.0004},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			metric, point := newSeededSum(pmetric.AggregationTemporalityCumulative, tc.valueType, tc.seed)
			opts := AdvanceOptions{
				Hint:       &MetricGenerationHint{Class: GenerationHintSeededRateCounter},
				Interval:   interval,
				Precision:  tc.precision,
				SeriesRate: tc.rate,
			}
			previous := tc.seed
			for range intervals {
				AdvanceDataPoint(point, rng, metric, DefaultDistribution, nil, opts)
				value := currentNumberDataPointValue(point)
				require.GreaterOrEqual(t, value, previous)
				if p, ok := tc.precision["test.seeded"]; ok {
					require.Equal(t, roundToPrecision(value, p), value)
				}
				previous = value
			}
			want := tc.rate * interval.Seconds()
			got := (previous - tc.seed) / intervals
			assert.InEpsilon(t, want, got, 0.02, "average increase per interval")
		})
	}
}

func TestSeededRateCounterZeroStaysZero(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, valueType := range []pmetric.NumberDataPointValueType{pmetric.NumberDataPointValueTypeInt, pmetric.NumberDataPointValueTypeDouble} {
		metric, point := newSeededSum(pmetric.AggregationTemporalityCumulative, valueType, 0)
		opts := AdvanceOptions{Hint: &MetricGenerationHint{Class: GenerationHintSeededRateCounter}, Interval: 10 * time.Second}
		for range 1000 {
			AdvanceDataPoint(point, rng, metric, DefaultDistribution, nil, opts)
			require.Zero(t, currentNumberDataPointValue(point))
		}
	}
}

func TestSeededRateCounterDeltaTemporality(t *testing.T) {
	const intervals = 20000
	rng := rand.New(rand.NewSource(7))
	metric, point := newSeededSum(pmetric.AggregationTemporalityDelta, pmetric.NumberDataPointValueTypeInt, 0)
	opts := AdvanceOptions{
		Hint:       &MetricGenerationHint{Class: GenerationHintSeededRateCounter},
		Interval:   10 * time.Second,
		SeriesRate: 0.25,
	}
	total := 0.0
	for range intervals {
		AdvanceDataPoint(point, rng, metric, DefaultDistribution, nil, opts)
		value := currentNumberDataPointValue(point)
		require.GreaterOrEqual(t, value, 0.0)
		total += value
	}
	assert.InEpsilon(t, 2.5, total/intervals, 0.02, "average value per interval")
}

func TestSeededRateInstanceVariation(t *testing.T) {
	metric, _ := newSeededSum(pmetric.AggregationTemporalityCumulative, pmetric.NumberDataPointValueTypeDouble, 0)
	plan := PlanNumberSeries(metric, seededHints, nil)
	identityHash := IdentityHash(metric.Name(), pcommon.NewMap())
	vary := func(value float64, instanceID int) float64 {
		point := pmetric.NewNumberDataPoint()
		point.SetDoubleValue(value)
		EmitForInstance(point, metric, InstanceEmitOptions{InstanceID: instanceID, IdentityHash: identityHash, Plan: plan})
		return point.DoubleValue()
	}

	factors := map[float64]struct{}{}
	for instanceID := range 20 {
		factor := vary(1000, instanceID) / 1000
		assert.GreaterOrEqual(t, factor, 0.5)
		assert.Less(t, factor, 1.5)
		factors[factor] = struct{}{}
		// The factor is stable: the same for every value, and zero stays zero.
		assert.InDelta(t, factor, vary(123456, instanceID)/123456, 1e-12)
		assert.Equal(t, vary(1000, instanceID), vary(1000, instanceID))
		assert.Zero(t, vary(0, instanceID))
	}
	assert.Len(t, factors, 20, "instances get different factors")
}

func newSeededSum(temporality pmetric.AggregationTemporality, valueType pmetric.NumberDataPointValueType, value float64) (pmetric.Metric, pmetric.NumberDataPoint) {
	metric := pmetric.NewMetric()
	metric.SetName("test.seeded")
	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(temporality)
	point := sum.DataPoints().AppendEmpty()
	if valueType == pmetric.NumberDataPointValueTypeInt {
		point.SetIntValue(int64(math.Round(value)))
	} else {
		point.SetDoubleValue(value)
	}
	return metric, point
}
