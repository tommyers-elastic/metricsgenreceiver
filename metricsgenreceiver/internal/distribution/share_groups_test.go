package distribution

import (
	"math"
	"testing"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// shareMember is a data point of a share group test template.
type shareMember struct {
	attrs map[string]any
	value any // int64 or float64
}

// shareTemplate returns a template with one metric per entry of metrics, in one scope.
func shareTemplate(t *testing.T, setup func(pmetric.Metric) pmetric.NumberDataPointSlice, metrics map[string][]shareMember, order ...string) pmetric.Metrics {
	t.Helper()
	template := pmetric.NewMetrics()
	ms := template.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for _, name := range order {
		m := ms.AppendEmpty()
		m.SetName(name)
		points := setup(m)
		for _, member := range metrics[name] {
			point := points.AppendEmpty()
			require.NoError(t, point.Attributes().FromRaw(member.attrs))
			switch v := member.value.(type) {
			case int64:
				point.SetIntValue(v)
			case float64:
				point.SetDoubleValue(v)
			}
		}
	}
	return template
}

// emitAll returns the value of every modelled data point of the template for the instance at
// time at, emitted like the receiver does: with one scratch for the instance and tick.
func emitAll(t *testing.T, plan *ModelPlan, template pmetric.Metrics, instanceID int, at time.Time) map[int]float64 {
	t.Helper()
	scratch := plan.GetScratch()
	defer plan.PutScratch(scratch)
	values := map[int]float64{}
	dp.ForEachDataPoint(&template, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
		if !plan.Handles(idx) {
			return
		}
		point := pmetric.NewNumberDataPoint()
		dataPoint.(pmetric.NumberDataPoint).CopyTo(point)
		point.SetStartTimestamp(pcommon.NewTimestampFromTime(modelTestStart))
		point.SetTimestamp(pcommon.NewTimestampFromTime(at))
		plan.Apply(point, idx, instanceID, scratch)
		values[idx] = currentNumberDataPointValue(point)
	})
	return values
}

func memoryStates(node string, used, free, cached, buffered int64) []shareMember {
	return []shareMember{
		{attrs: map[string]any{"node": node, "state": "used"}, value: used},
		{attrs: map[string]any{"node": node, "state": "free"}, value: free},
		{attrs: map[string]any{"node": node, "state": "cached"}, value: cached},
		{attrs: map[string]any{"node": node, "state": "buffered"}, value: buffered},
	}
}

func TestShareGaugeIntGroupsAddUpToTheirTotal(t *testing.T) {
	// Two groups, one per value of the node attribute; the buffered state of the first is zero.
	members := append(memoryStates("a", 3000000000, 1000000000, 500000000, 0), memoryStates("b", 7, 2, 1, 1)...)
	template := shareTemplate(t, upDownSumSetup, map[string][]shareMember{"test.usage": members}, "test.usage")
	hints := map[string]MetricGenerationHint{"test.usage": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state", "variation": "0.5"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	require.Len(t, plan.groups, 2)
	assert.Equal(t, []int{0, 1, 2, 3}, plan.groups[0].members)
	assert.Equal(t, []int{4, 5, 6, 7}, plan.groups[1].members)

	totals := []float64{4500000000, 11}
	for instance := 0; instance < 10; instance++ {
		seen := make([]seriesSpan, 8)
		for i := 0; i < 2000; i++ {
			at := modelTestStart.Add(time.Duration(i) * 61 * time.Second)
			values := emitAll(t, plan, template, instance, at)
			for g, group := range plan.groups {
				sum := 0.0
				for _, idx := range group.members {
					require.GreaterOrEqual(t, values[idx], 0.0)
					require.Equal(t, math.Round(values[idx]), values[idx])
					sum += values[idx]
					seen[idx].add(values[idx])
				}
				require.Equal(t, totals[g], sum, "instance %d sample %d group %d", instance, i, g)
			}
			// A zero seed stays zero.
			require.Zero(t, values[3])
			// Shares stay within f_s * (1 +- variation * (1 - f_s)), plus rounding.
			for _, idx := range plan.groups[0].members {
				f := meanShare(plan, 0, idx, instance)
				share := values[idx] / totals[0]
				require.InDelta(t, f, share, 0.5*f*(1-f)+1e-9, "instance %d sample %d member %d", instance, i, idx)
			}
		}
		// The non-zero members of the large group move.
		for _, idx := range []int{0, 1, 2} {
			assert.Greater(t, seen[idx].max-seen[idx].min, 1e6, "instance %d member %d", instance, idx)
		}
	}
}

// meanShare returns f_s of the member at template index idx of group g for the instance.
func meanShare(plan *ModelPlan, g int, idx int, instanceID int) float64 {
	sum := 0.0
	for _, member := range plan.groups[g].members {
		s := &plan.series[member]
		sum += s.seed * s.instanceFactor(instanceID)
	}
	s := &plan.series[idx]
	return s.seed * s.instanceFactor(instanceID) / sum
}

type seriesSpan struct {
	min, max float64
	set      bool
}

func (s *seriesSpan) add(v float64) {
	if !s.set {
		s.min, s.max, s.set = v, v, true
	}
	s.min, s.max = math.Min(s.min, v), math.Max(s.max, v)
}

func TestShareGaugeDoublesAddUpToTotal(t *testing.T) {
	members := []shareMember{
		{attrs: map[string]any{"state": "user"}, value: 0.25},
		{attrs: map[string]any{"state": "system"}, value: 0.125},
		{attrs: map[string]any{"state": "idle"}, value: 0.5},
		{attrs: map[string]any{"state": "steal"}, value: 0.001},
	}
	template := shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.utilization": members}, "test.utilization")
	hints := map[string]MetricGenerationHint{"test.utilization": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state", "total": "1"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	for instance := 0; instance < 10; instance++ {
		for i := 0; i < 1000; i++ {
			values := emitAll(t, plan, template, instance, modelTestStart.Add(time.Duration(i)*13*time.Second))
			sum := 0.0
			for _, value := range values {
				require.GreaterOrEqual(t, value, 0.0)
				// Rounded to the inferred precision of 3 decimals.
				require.InDelta(t, math.Round(value*1000)/1000, value, 1e-12)
				sum += value
			}
			require.InDelta(t, 1, sum, 1e-3, "instance %d sample %d", instance, i)
		}
	}
}

func TestShareGaugeWithoutPrecisionAddsUpToTotal(t *testing.T) {
	// All seeds are zero apart from one, so no precision is inferred from the others; with total
	// unset the total is the seed.
	members := []shareMember{
		{attrs: map[string]any{"state": "a"}, value: 1.0 / 3},
		{attrs: map[string]any{"state": "b"}, value: 0.0},
	}
	template := shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.metric": members}, "test.metric")
	hints := map[string]MetricGenerationHint{"test.metric": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	values := emitAll(t, plan, template, 1, modelTestStart.Add(time.Hour))
	assert.InDelta(t, 1.0/3, values[0]+values[1], 1e-15)
	assert.Zero(t, values[1])
}

func TestShareGaugeRoundingNeverNegative(t *testing.T) {
	g := shareGroup{total: 2, valueType: pmetric.NumberDataPointValueTypeInt}
	values := []float64{0.5, 0.5, 0.5, 0.5}
	g.roundToTotal(values)
	assert.Equal(t, []float64{0, 0, 1, 1}, values)

	g = shareGroup{total: 3, valueType: pmetric.NumberDataPointValueTypeInt}
	values = []float64{0.4, 0.4, 2.2}
	g.roundToTotal(values)
	assert.Equal(t, []float64{0, 0, 3}, values)
}

func TestShareGaugeVariationKeyKeepsMetricsConsistent(t *testing.T) {
	usage := []shareMember{
		{attrs: map[string]any{"state": "used"}, value: int64(6000)},
		{attrs: map[string]any{"state": "free"}, value: int64(3000)},
		{attrs: map[string]any{"state": "cached"}, value: int64(1000)},
	}
	utilization := []shareMember{
		{attrs: map[string]any{"state": "used"}, value: 0.6},
		{attrs: map[string]any{"state": "free"}, value: 0.3},
		{attrs: map[string]any{"state": "cached"}, value: 0.1},
	}
	template := pmetric.NewMetrics()
	for name, members := range map[string][]shareMember{"test.usage": usage, "test.utilization": utilization} {
		part := shareTemplate(t, gaugeSetup, map[string][]shareMember{name: members}, name)
		part.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).CopyTo(appendMetric(template))
	}
	params := map[string]string{"share_by": "state", "variation_key": "memory"}
	hints := map[string]MetricGenerationHint{
		"test.usage":       {Class: GenerationHintShareGauge, Params: params},
		"test.utilization": {Class: GenerationHintShareGauge, Params: params},
	}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	require.Len(t, plan.groups, 2)
	usageGroup, utilizationGroup := plan.groups[0], plan.groups[1]
	if plan.series[usageGroup.members[0]].seed < 1 {
		usageGroup, utilizationGroup = utilizationGroup, usageGroup
	}
	for instance := 0; instance < 5; instance++ {
		for i := 0; i < 200; i++ {
			at := modelTestStart.Add(time.Duration(i) * 47 * time.Second)
			// The unrounded shares agree.
			a := make([]float64, 3)
			b := make([]float64, 3)
			plan.shares(&usageGroup, instance, at.UnixNano(), a, make([]float64, 3))
			plan.shares(&utilizationGroup, instance, at.UnixNano(), b, make([]float64, 3))
			for k := range a {
				require.InDelta(t, a[k], b[k], 1e-12)
			}
			// The emitted values agree within the rounding of the utilization to 1 decimal, which
			// may add a residual of 0.1 to its largest member.
			values := emitAll(t, plan, template, instance, at)
			for k := range usageGroup.members {
				require.InDelta(t, values[usageGroup.members[k]]/10000, values[utilizationGroup.members[k]], 0.15+1e-3)
			}
		}
	}
}

func appendMetric(template pmetric.Metrics) pmetric.Metric {
	if template.ResourceMetrics().Len() == 0 {
		template.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	}
	return template.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty()
}

func TestShareGaugeGroupsAreScopedAndIndexedLikeTheTemplate(t *testing.T) {
	template := pmetric.NewMetrics()
	for scope := 0; scope < 2; scope++ {
		ms := template.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
		histogram := ms.AppendEmpty()
		histogram.SetName("test.histogram")
		histogram.SetEmptyHistogram().DataPoints().AppendEmpty()
		histogram.Histogram().DataPoints().AppendEmpty()
		m := ms.AppendEmpty()
		m.SetName("test.share")
		points := m.SetEmptyGauge().DataPoints()
		for _, state := range []string{"a", "b"} {
			point := points.AppendEmpty()
			point.Attributes().PutStr("state", state)
			point.SetIntValue(5)
		}
	}
	hints := map[string]MetricGenerationHint{"test.share": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	// Equal attributes in different resources (and scopes) are different groups.
	require.Len(t, plan.groups, 2)
	assert.Equal(t, []int{2, 3}, plan.groups[0].members)
	assert.Equal(t, []int{6, 7}, plan.groups[1].members)
	var shareIndexes []int
	dp.ForEachDataPoint(&template, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, _ dp.DataPoint) {
		if m.Name() == "test.share" {
			shareIndexes = append(shareIndexes, idx)
		}
		assert.Equal(t, m.Name() == "test.share", plan.Handles(idx), idx)
	})
	assert.Equal(t, []int{2, 3, 6, 7}, shareIndexes)
	values := emitAll(t, plan, template, 4, modelTestStart.Add(time.Minute))
	assert.Equal(t, 10.0, values[2]+values[3])
	assert.Equal(t, 10.0, values[6]+values[7])
}

func TestShareGaugeStartupErrors(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(pmetric.Metric) pmetric.NumberDataPointSlice
		members   []shareMember
		params    map[string]string
		wantError string
	}{
		{
			name:      "share_by missing",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a"}, value: int64(1)}},
			params:    map[string]string{},
			wantError: `metric "test.share" with generation hint share_gauge: parameter metricsgen.hint.share_by is required`,
		},
		{
			name:      "data point without the share_by attribute",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a", "cpu": "0"}, value: int64(1)}, {attrs: map[string]any{"cpu": "0"}, value: int64(1)}},
			wantError: `metric "test.share" with generation hint share_gauge: the data point with attributes {cpu=0} has no attribute "state" named by metricsgen.hint.share_by`,
		},
		{
			name:      "duplicate member",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a"}, value: int64(1)}, {attrs: map[string]any{"state": "a"}, value: int64(2)}},
			wantError: `metric "test.share" with generation hint share_gauge: more than one data point has the attributes {state=a}`,
		},
		{
			name:      "negative seed",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a", "cpu": "1"}, value: int64(-1)}},
			wantError: `metric "test.share" with generation hint share_gauge: the data point with attributes {cpu=1, state=a} has a negative value (-1)`,
		},
		{
			name:      "mixed value types",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a"}, value: int64(1)}, {attrs: map[string]any{"state": "b"}, value: 0.5}},
			wantError: `metric "test.share" with generation hint share_gauge: the share group with attributes {} mixes int and double values`,
		},
		{
			name:      "total without seeds",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a", "cpu": "0"}, value: int64(0)}},
			params:    map[string]string{"share_by": "state", "total": "5"},
			wantError: `metric "test.share" with generation hint share_gauge: the share group with attributes {cpu=0} has metricsgen.hint.total 5 but all its values are 0`,
		},
		{
			name:      "int total above 2^53",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a"}, value: int64(1)}},
			params:    map[string]string{"share_by": "state", "total": "1e16"},
			wantError: `metric "test.share" with generation hint share_gauge: the int share group with attributes {} adds up to more than 2^53 (9007199254740992), the largest total up to which int values add up exactly`,
		},
		{
			name:      "int seeds above 2^53",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a", "cpu": "0"}, value: int64(1 << 53)}, {attrs: map[string]any{"state": "b", "cpu": "0"}, value: int64(1)}},
			wantError: `metric "test.share" with generation hint share_gauge: the int share group with attributes {cpu=0} adds up to more than 2^53 (9007199254740992), the largest total up to which int values add up exactly`,
		},
		{
			name:      "negative total",
			setup:     gaugeSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a"}, value: int64(1)}},
			params:    map[string]string{"share_by": "state", "total": "-5"},
			wantError: `metric "test.share" with generation hint share_gauge: parameter metricsgen.hint.total must be a number >= 0, got "-5"`,
		},
		{
			name:      "counter",
			setup:     counterSetup,
			members:   []shareMember{{attrs: map[string]any{"state": "a"}, value: int64(1)}},
			wantError: `metric "test.share" with generation hint share_gauge: the metric must be a gauge or a non-monotonic sum`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			template := shareTemplate(t, tc.setup, map[string][]shareMember{"test.share": tc.members}, "test.share")
			params := tc.params
			if params == nil {
				params = map[string]string{"share_by": "state"}
			}
			_, err := PlanModels(&template, map[string]MetricGenerationHint{"test.share": {Class: GenerationHintShareGauge, Params: params}}, pmetric.AggregationTemporalityUnspecified)
			require.EqualError(t, err, tc.wantError)
		})
	}
}

func TestShareGaugeSingleMemberEqualsTotal(t *testing.T) {
	template := shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.share": {{attrs: map[string]any{"state": "a"}, value: int64(42)}}}, "test.share")
	plan, err := PlanModels(&template, map[string]MetricGenerationHint{"test.share": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state", "total": "50"}}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		assert.Equal(t, 50.0, emitAll(t, plan, template, i, modelTestStart.Add(time.Duration(i)*time.Minute))[0])
	}
}

func TestShareGaugeIntTotalUpTo2To53(t *testing.T) {
	// An int group may add up to 2^53 exactly; a double group may add up to more.
	template := shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.share": {
		{attrs: map[string]any{"state": "a"}, value: int64(1 << 52)},
		{attrs: map[string]any{"state": "b"}, value: int64(1<<52 - 1)},
		{attrs: map[string]any{"state": "c"}, value: int64(1)},
	}}, "test.share")
	plan, err := PlanModels(&template, map[string]MetricGenerationHint{"test.share": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state", "variation": "1"}}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	for instance := 0; instance < 20; instance++ {
		values := emitAll(t, plan, template, instance, modelTestStart.Add(time.Duration(instance)*time.Minute))
		sum := int64(0)
		for _, v := range values {
			sum += int64(v)
		}
		require.Equal(t, int64(1<<53), sum, "instance %d", instance)
	}
	doubles := shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.share": {{attrs: map[string]any{"state": "a"}, value: 0.5}}}, "test.share")
	_, err = PlanModels(&doubles, map[string]MetricGenerationHint{"test.share": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state", "total": "1e20"}}}, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
}

func TestShareGroupKeysAreUnambiguous(t *testing.T) {
	// The attributes besides share_by differ ({a="x|attr=b:str:y"} and {a=x, b=y}), so these
	// are two groups, even though a flat concatenation of the attributes would be the same.
	template := shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.share": {
		{attrs: map[string]any{"state": "used", "a": "x|attr=b:str:y"}, value: int64(10)},
		{attrs: map[string]any{"state": "free", "a": "x", "b": "y"}, value: int64(30)},
	}}, "test.share")
	hints := map[string]MetricGenerationHint{"test.share": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	require.Len(t, plan.groups, 2)
	values := emitAll(t, plan, template, 3, modelTestStart.Add(time.Minute))
	assert.Equal(t, 10.0, values[0])
	assert.Equal(t, 30.0, values[1])

	// Members whose share_by values are different maps are different members, even when a flat
	// concatenation of the maps would be the same.
	template = shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.share": {
		{attrs: map[string]any{"state": map[string]any{"a": "x,b:str:y"}}, value: int64(10)},
		{attrs: map[string]any{"state": map[string]any{"a": "x", "b": "y"}}, value: int64(30)},
	}}, "test.share")
	plan, err = PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	require.Len(t, plan.groups, 1)
	assert.Equal(t, []int{0, 1}, plan.groups[0].members)

	// Equal attributes still make one group with distinct members, whatever the map order, and
	// values of different types are different.
	template = shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.share": {
		{attrs: map[string]any{"state": int64(1), "n": map[string]any{"x": "1", "y": []any{"a", int64(2)}}}, value: int64(10)},
		{attrs: map[string]any{"state": 1.0, "n": map[string]any{"y": []any{"a", int64(2)}, "x": "1"}}, value: int64(30)},
		{attrs: map[string]any{"state": "1", "n": map[string]any{"x": "1", "y": []any{"a", int64(2)}}}, value: int64(30)},
	}}, "test.share")
	plan, err = PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	require.Len(t, plan.groups, 1)
	assert.Len(t, plan.groups[0].members, 3)
	duplicate := shareTemplate(t, gaugeSetup, map[string][]shareMember{"test.share": {
		{attrs: map[string]any{"state": []any{"a", "b"}}, value: int64(10)},
		{attrs: map[string]any{"state": []any{"a", "b"}}, value: int64(30)},
	}}, "test.share")
	_, err = PlanModels(&duplicate, hints, pmetric.AggregationTemporalityUnspecified)
	require.ErrorContains(t, err, "more than one data point has the attributes")
}

func TestCanonicalValueEncodingIsUnambiguous(t *testing.T) {
	values := []any{
		"", "a", "ab", "s1:a", "1", int64(1), 1.0, true, "true", []byte("a"), []byte{},
		[]any{}, []any{"a"}, []any{"a", "b"}, []any{"ab"}, []any{[]any{"a"}, "b"}, []any{"a", []any{"b"}},
		map[string]any{}, map[string]any{"a": "b"}, map[string]any{"a": "b", "c": "d"}, map[string]any{"a": "bs1:cs1:d"},
		map[string]any{"a": "x,b:str:y"}, map[string]any{"a": "x", "b": "y"},
		map[string]any{"a": map[string]any{"b": "c"}}, map[string]any{"a": []any{"b", "c"}}, nil,
	}
	seen := map[string]int{}
	for i, raw := range values {
		value := pcommon.NewValueEmpty()
		require.NoError(t, value.FromRaw(raw))
		encoding := string(appendCanonicalValue(nil, value))
		other, ok := seen[encoding]
		require.False(t, ok, "values %d and %d have the same encoding %q", other, i, encoding)
		seen[encoding] = i
	}
}

func TestShareGaugeVariationKeyParameters(t *testing.T) {
	template := pmetric.NewMetrics()
	for _, name := range []string{"test.usage", "test.utilization"} {
		part := shareTemplate(t, gaugeSetup, map[string][]shareMember{name: {
			{attrs: map[string]any{"state": "used", "kind": "x"}, value: 0.6},
			{attrs: map[string]any{"state": "free", "kind": "x"}, value: 0.4},
		}}, name)
		part.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).CopyTo(appendMetric(template))
	}
	base := map[string]string{"share_by": "state", "variation_key": "memory"}
	for _, tc := range []struct {
		param, value, want string
	}{
		{param: "period", value: "20m", want: `different metricsgen.hint.period ("10m0s" and "20m0s")`},
		{param: "instance_spread", value: "3", want: `different metricsgen.hint.instance_spread ("2" and "3")`},
		{param: "variation", value: "0.9", want: `different metricsgen.hint.variation ("0.25" and "0.9")`},
		{param: "share_by", value: "kind", want: `different metricsgen.hint.share_by ("state" and "kind")`},
	} {
		t.Run(tc.param, func(t *testing.T) {
			other := map[string]string{"share_by": "state", "variation_key": "memory"}
			other[tc.param] = tc.value
			hints := map[string]MetricGenerationHint{
				"test.usage":       {Class: GenerationHintShareGauge, Params: base},
				"test.utilization": {Class: GenerationHintShareGauge, Params: other},
			}
			_, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
			require.ErrorContains(t, err, `metrics "test.usage" and "test.utilization" have the same variation key "memory"`)
			require.ErrorContains(t, err, tc.want)
		})
	}
	// A total does not change the shares, so it may differ.
	hints := map[string]MetricGenerationHint{
		"test.usage":       {Class: GenerationHintShareGauge, Params: base},
		"test.utilization": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state", "variation_key": "memory", "total": "1"}},
	}
	_, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	// A bounded_gauge with the same key must agree on period and instance_spread only.
	hints["test.utilization"] = MetricGenerationHint{Class: GenerationHintBoundedGauge, Params: map[string]string{"variation_key": "memory", "variation": "0.9"}}
	_, err = PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	hints["test.utilization"] = MetricGenerationHint{Class: GenerationHintBoundedGauge, Params: map[string]string{"variation_key": "memory", "period": "1h"}}
	_, err = PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.ErrorContains(t, err, `different metricsgen.hint.period ("10m0s" and "1h0m0s")`)
}

func TestShareNeverFallsFarBelowItsMeanShare(t *testing.T) {
	// The worst case for a member: its noise at -1 and every other member's at +1. With
	// variation 1, the share would fall to f_s^2; the cap keeps it above f_s / 1000.
	g := &shareGroup{params: &modelParams{variation: 1}}
	for _, f := range []float64{1e-9, 1e-6, 1e-3, 0.1, 0.5} {
		shares := []float64{f, 1 - f}
		g.shareFormula(shares, []float64{-1, 1}, 1)
		assert.Greater(t, shares[0], f/1000, "f_s = %v", f)
		assert.Less(t, shares[0], f, "f_s = %v", f)
		assert.InDelta(t, 1, shares[0]+shares[1], 1e-15)
		// The documented bound with the given variation still holds.
		assert.GreaterOrEqual(t, shares[0], f*(1-(1-f))-1e-18)
	}
}

func TestShareGroupsNeverSpanScopes(t *testing.T) {
	// The same metric with the same attributes in two scopes of one resource and in a second
	// resource forms one group per scope: share group values are cached per emission on the
	// assumption that all members of a group have the same timestamps, and the receiver's
	// stagger_scopes offsets differ between scopes.
	template := pmetric.NewMetrics()
	for _, scopes := range []int{2, 1} {
		rm := template.ResourceMetrics().AppendEmpty()
		for scope := 0; scope < scopes; scope++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName("test.scope")
			m := sm.Metrics().AppendEmpty()
			m.SetName("test.share")
			points := m.SetEmptyGauge().DataPoints()
			for _, state := range []string{"a", "b"} {
				point := points.AppendEmpty()
				point.Attributes().PutStr("state", state)
				point.SetIntValue(5)
			}
		}
	}
	hints := map[string]MetricGenerationHint{"test.share": {Class: GenerationHintShareGauge, Params: map[string]string{"share_by": "state"}}}
	plan, err := PlanModels(&template, hints, pmetric.AggregationTemporalityUnspecified)
	require.NoError(t, err)
	require.Len(t, plan.groups, 3)
	scopeOf := map[int]int{}
	forEachScopeDataPoint(&template, func(idx int, scope int, _ pmetric.Metric, _ dp.DataPoint) {
		scopeOf[idx] = scope
	})
	for _, g := range plan.groups {
		for _, member := range g.members {
			assert.Equal(t, scopeOf[g.members[0]], scopeOf[member])
		}
	}
	values := emitAll(t, plan, template, 2, modelTestStart.Add(time.Minute))
	for _, g := range plan.groups {
		assert.Equal(t, 10.0, values[g.members[0]]+values[g.members[1]])
	}
}

func TestForEachScopeDataPointVisitsLikeForEachDataPoint(t *testing.T) {
	// Resources with several scopes, an empty scope and an empty resource, and every metric type
	// with several data points: forEachScopeDataPoint visits the number data points with the
	// indexes of dp.ForEachDataPoint, and numbers the scopes in template order.
	template := pmetric.NewMetrics()
	add := func(ms pmetric.MetricSlice, name string, typ pmetric.MetricType, points int) {
		m := ms.AppendEmpty()
		m.SetName(name)
		for i := 0; i < points; i++ {
			var attrs pcommon.Map
			switch typ {
			case pmetric.MetricTypeGauge:
				if i == 0 {
					m.SetEmptyGauge()
				}
				point := m.Gauge().DataPoints().AppendEmpty()
				point.SetIntValue(1)
				attrs = point.Attributes()
			case pmetric.MetricTypeSum:
				if i == 0 {
					m.SetEmptySum()
				}
				point := m.Sum().DataPoints().AppendEmpty()
				point.SetDoubleValue(1)
				attrs = point.Attributes()
			case pmetric.MetricTypeHistogram:
				if i == 0 {
					m.SetEmptyHistogram()
				}
				attrs = m.Histogram().DataPoints().AppendEmpty().Attributes()
			case pmetric.MetricTypeExponentialHistogram:
				if i == 0 {
					m.SetEmptyExponentialHistogram()
				}
				attrs = m.ExponentialHistogram().DataPoints().AppendEmpty().Attributes()
			case pmetric.MetricTypeSummary:
				if i == 0 {
					m.SetEmptySummary()
				}
				attrs = m.Summary().DataPoints().AppendEmpty().Attributes()
			}
			attrs.PutInt("i", int64(i))
		}
	}
	scopeNames := []string{}
	for r := 0; r < 3; r++ {
		rm := template.ResourceMetrics().AppendEmpty()
		if r == 1 {
			continue // a resource without scopes
		}
		for s := 0; s < 3; s++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			name := string(rune('a'+r)) + string(rune('0'+s))
			sm.Scope().SetName(name)
			scopeNames = append(scopeNames, name)
			if s == 1 {
				continue // a scope without metrics
			}
			metrics := sm.Metrics()
			add(metrics, "histogram", pmetric.MetricTypeHistogram, 2)
			add(metrics, "gauge", pmetric.MetricTypeGauge, 2)
			add(metrics, "exponential_histogram", pmetric.MetricTypeExponentialHistogram, 3)
			add(metrics, "sum", pmetric.MetricTypeSum, 2)
			add(metrics, "summary", pmetric.MetricTypeSummary, 1)
			metrics.AppendEmpty().SetName("empty")
			add(metrics, "gauge2", pmetric.MetricTypeGauge, 1)
		}
	}
	type visit struct {
		scope  string
		metric string
		i      int64
	}
	want := map[int]visit{}
	dp.ForEachDataPoint(&template, func(idx int, _ pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		if _, ok := dataPoint.(pmetric.NumberDataPoint); !ok {
			return
		}
		i, _ := dataPoint.Attributes().Get("i")
		want[idx] = visit{scope: is.Name(), metric: m.Name(), i: i.Int()}
	})
	got := map[int]visit{}
	forEachScopeDataPoint(&template, func(idx int, scope int, m pmetric.Metric, dataPoint dp.DataPoint) {
		i, _ := dataPoint.Attributes().Get("i")
		got[idx] = visit{scope: scopeNames[scope], metric: m.Name(), i: i.Int()}
	})
	assert.Len(t, want, 4*5)
	assert.Equal(t, want, got)
}
