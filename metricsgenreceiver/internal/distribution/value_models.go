package distribution

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// modelClasses are the hint classes whose values are computed by a ModelPlan at emission
// instead of by advancing the template and varying each instance's copy.
var modelClasses = map[GenerationHintClass]bool{
	GenerationHintBoundedGauge: true,
	GenerationHintShareGauge:   true,
}

// shareClasses are the model hint classes whose series form share groups.
var shareClasses = map[GenerationHintClass]bool{
	GenerationHintShareGauge: true,
}

// noisePeriodScales are the periods of the three sinusoids of the noise, relative to the period
// parameter.
var noisePeriodScales = [3]float64{1, 0.37, 0.11}

const (
	// Components of the hashes the models derive from a series and an instance.
	hashComponentInstanceFactor = 0
	hashComponentNoise          = 1 // and up, two per sinusoid
)

// ModelPlan holds what the stateless value models (see modelClasses) need to compute the
// value of a data point for an instance at emission: the seed value of each modelled series,
// read from the template before it is advanced, its parsed hint parameters, its variation
// identity and its precision. The value is a function of these, the instance and the data
// point's timestamp, so the models keep no per-series or per-instance state.
type ModelPlan struct {
	// series is indexed like the template's data points in ForEachDataPoint order; entries of
	// data points that are not modelled have an empty class.
	series []modelSeries
	// groups are the share groups of the template, in the order of their first member.
	groups []shareGroup
	// memberCount is the number of members of all groups, and maxGroupSize the size of the
	// largest group.
	memberCount  int
	maxGroupSize int
	// scratch recycles ModelScratch values between emissions.
	scratch sync.Pool
}

type modelSeries struct {
	class  GenerationHintClass
	params *modelParams
	// logInstanceSpread is the natural logarithm of params.instanceSpread.
	logInstanceSpread float64
	seed              float64
	// identity is IdentityHash of the variation key (the metric name by default) and the data
	// point's attributes: series with equal identities vary together.
	identity  uint64
	precision NumberPrecision
	// group is the index of the series' share group in ModelPlan.groups and member its position
	// in the group; only set for share classes.
	group  int
	member int
}

// PlanModels plans the stateless value models of a template. It must be called before the
// template is advanced. temporalityOverride is the scenario's temporality override, or
// AggregationTemporalityUnspecified. It returns nil when no metric uses a model hint class.
func PlanModels(metrics *pmetric.Metrics, hints map[string]MetricGenerationHint, temporalityOverride pmetric.AggregationTemporality) (*ModelPlan, error) {
	params := map[string]*modelParams{}
	for name, hint := range hints {
		if !modelClasses[hint.Class] {
			continue
		}
		p, err := hint.parseParams(name)
		if err != nil {
			return nil, err
		}
		params[name] = &p
	}
	if len(params) == 0 {
		return nil, nil
	}
	if err := checkVariationKeys(hints, params); err != nil {
		return nil, err
	}
	precision := InferPrecision(metrics)
	plan := &ModelPlan{series: make([]modelSeries, metrics.DataPointCount())}
	var errs []error
	failed := map[string]bool{}
	groups := newShareGroupBuilder()
	forEachScopeDataPoint(metrics, func(idx int, scope int, m pmetric.Metric, dataPoint dp.DataPoint) {
		p, ok := params[m.Name()]
		if !ok || failed[m.Name()] {
			return
		}
		class := hints[m.Name()].Class
		fail := func(err error) {
			errs = append(errs, fmt.Errorf("metric %q with generation hint %s: %w", m.Name(), class, err))
			failed[m.Name()] = true
		}
		if err := checkModelMetric(m, class, temporalityOverride); err != nil {
			fail(err)
			return
		}
		v, ok := dataPoint.(pmetric.NumberDataPoint)
		if !ok || v.ValueType() == pmetric.NumberDataPointValueTypeEmpty {
			fail(errors.New("every data point must have a number value"))
			return
		}
		if class == GenerationHintBoundedGauge && v.ValueType() == pmetric.NumberDataPointValueTypeInt && p.hasMin && p.hasMax && math.Ceil(p.min) > math.Floor(p.max) {
			fail(fmt.Errorf("the metric has int values but there is no whole number between %s%s (%v) and %s%s (%v)", HintMetadataPrefix, HintParamMin, p.min, HintMetadataPrefix, HintParamMax, p.max))
			return
		}
		key := p.variationKey
		if key == "" {
			key = m.Name()
		}
		series := modelSeries{
			class:             class,
			params:            p,
			logInstanceSpread: math.Log(p.instanceSpread),
			seed:              currentNumberDataPointValue(v),
			identity:          IdentityHash(key, v.Attributes()),
			precision:         resolvePrecision(m, precision),
		}
		if shareClasses[class] {
			if err := groups.add(&series, idx, scope, m, v); err != nil {
				fail(err)
				return
			}
		}
		plan.series[idx] = series
	})
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if err := groups.build(plan); err != nil {
		return nil, err
	}
	plan.scratch.New = func() any { return plan.newScratch() }
	return plan, nil
}

// forEachScopeDataPoint calls visit for every number data point of metrics with its index in
// dp.ForEachDataPoint order and the index of its scope among all scopes of metrics.
func forEachScopeDataPoint(metrics *pmetric.Metrics, visit func(idx int, scope int, m pmetric.Metric, dataPoint dp.DataPoint)) {
	idx, scope := 0, 0
	resourceMetrics := metrics.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		scopeMetrics := resourceMetrics.At(i).ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			ms := scopeMetrics.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				var points pmetric.NumberDataPointSlice
				//exhaustive:enforce
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					points = m.Gauge().DataPoints()
				case pmetric.MetricTypeSum:
					points = m.Sum().DataPoints()
				case pmetric.MetricTypeHistogram:
					idx += m.Histogram().DataPoints().Len()
					continue
				case pmetric.MetricTypeExponentialHistogram:
					idx += m.ExponentialHistogram().DataPoints().Len()
					continue
				case pmetric.MetricTypeSummary:
					idx += m.Summary().DataPoints().Len()
					continue
				case pmetric.MetricTypeEmpty:
					continue
				}
				for l := 0; l < points.Len(); l++ {
					visit(idx, scope, m, points.At(l))
					idx++
				}
			}
			scope++
		}
	}
}

// checkModelMetric checks that the metric type suits the model hint class.
func checkModelMetric(m pmetric.Metric, class GenerationHintClass, _ pmetric.AggregationTemporality) error {
	switch class {
	case GenerationHintBoundedGauge, GenerationHintShareGauge:
		if m.Type() == pmetric.MetricTypeGauge || (m.Type() == pmetric.MetricTypeSum && !m.Sum().IsMonotonic()) {
			return nil
		}
		return errors.New("the metric must be a gauge or a non-monotonic sum")
	}
	return nil
}

// Handles reports whether the plan computes the value of the template data point at index idx.
// It is false for a nil plan.
func (p *ModelPlan) Handles(idx int) bool {
	return p != nil && p.series[idx].class != ""
}

// Apply sets v, an instance's copy of the template data point at index idx, to the value of its
// model for the instance at v's timestamp. Callers set the data point's timestamps before.
// scratch must come from GetScratch for the emission of this instance; it may be nil when the
// plan has no share groups.
func (p *ModelPlan) Apply(v pmetric.NumberDataPoint, idx int, instanceID int, scratch *ModelScratch) {
	s := &p.series[idx]
	switch s.class {
	case GenerationHintBoundedGauge:
		value := s.boundedGaugeValue(instanceID, int64(v.Timestamp()))
		value = roundModelValue(value, v.ValueType(), s.precision)
		setModelValue(v, s.params.clamp(value, v.ValueType()))
	case GenerationHintShareGauge:
		setModelValue(v, p.memberValue(s, instanceID, int64(v.Timestamp()), scratch))
	}
}

// boundedGaugeValue returns seed * k * (1 + variation * n(t)), clamped to [min, max], where k is
// the instance's factor and n the series' noise for the instance.
func (s *modelSeries) boundedGaugeValue(instanceID int, t int64) float64 {
	centre := float64(s.seed * s.instanceFactor(instanceID))
	n := newNoise(s.identity, instanceID, s.params.period).value(t)
	value := float64(centre * float64(1+float64(s.params.variation*n)))
	return s.params.clamp(value, pmetric.NumberDataPointValueTypeDouble)
}

// instanceFactor returns the stable factor of the instance for the series: log-uniform in
// [1/instance_spread, instance_spread].
func (s *modelSeries) instanceFactor(instanceID int) float64 {
	if s.logInstanceSpread == 0 {
		return 1
	}
	unit := hashToUnitInterval(modelHash(s.identity, instanceID, hashComponentInstanceFactor))
	return math.Exp(float64(s.logInstanceSpread * float64(2*unit-1)))
}

// clamp limits value to [min, max] when they are set. For int values, the bounds are rounded
// inwards to whole numbers.
func (p *modelParams) clamp(value float64, valueType pmetric.NumberDataPointValueType) float64 {
	if p.hasMax {
		upper := p.max
		if valueType == pmetric.NumberDataPointValueTypeInt {
			upper = math.Floor(upper)
		}
		value = math.Min(value, upper)
	}
	if p.hasMin {
		lower := p.min
		if valueType == pmetric.NumberDataPointValueTypeInt {
			lower = math.Ceil(lower)
		}
		value = math.Max(value, lower)
	}
	return value
}

// roundModelValue rounds value to a whole number for int values and to the precision for double
// values when one is set.
func roundModelValue(value float64, valueType pmetric.NumberDataPointValueType, precision NumberPrecision) float64 {
	switch valueType {
	case pmetric.NumberDataPointValueTypeInt:
		return math.Round(value)
	case pmetric.NumberDataPointValueTypeDouble:
		if precision.Set {
			return roundToPrecision(value, precision.Decimals)
		}
	}
	return value
}

func setModelValue(v pmetric.NumberDataPoint, value float64) {
	switch v.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		v.SetDoubleValue(value)
	case pmetric.NumberDataPointValueTypeInt:
		v.SetIntValue(modelIntValue(value))
	}
}

// modelIntValue converts a whole number to int64, clamping it to the int64 range (and NaN to 0):
// converting a float64 outside that range is implementation-defined in Go and differs between
// amd64 and arm64, so the clamp keeps the output the same on every architecture.
func modelIntValue(value float64) int64 {
	switch {
	case math.IsNaN(value):
		return 0
	case value >= float64(math.MaxInt64):
		// float64(math.MaxInt64) is 2^63, the smallest float64 above the int64 range.
		return math.MaxInt64
	case value <= float64(math.MinInt64):
		return math.MinInt64
	}
	return int64(value)
}

// noise is a smooth periodic process in [-1, 1]: the mean of three sinusoids with periods of
// about period * {1, 0.37, 0.11}, each scaled by a hashed factor in [0.75, 1.25), and hashed
// phases.
type noise [3]noiseComponent

type noiseComponent struct {
	// period is the period of the sinusoid in nanoseconds.
	period int64
	phase  float64
}

func newNoise(identity uint64, instanceID int, period time.Duration) noise {
	var n noise
	for i := range n {
		scale := 0.75 + float64(0.5*hashToUnitInterval(modelHash(identity, instanceID, uint64(hashComponentNoise+2*i))))
		n[i].period = max(1, int64(float64(float64(period)*noisePeriodScales[i])*scale))
		n[i].phase = float64(2 * math.Pi * hashToUnitInterval(modelHash(identity, instanceID, uint64(hashComponentNoise+2*i+1))))
	}
	return n
}

// angle returns the angle of the sinusoid at t (unix nanoseconds). The position within the
// period is computed in integer nanoseconds, so it is exact for any time.
func (c noiseComponent) angle(t int64) float64 {
	return float64(2*math.Pi*float64(positiveMod(t, c.period))/float64(c.period)) + c.phase
}

// value returns the noise at t (unix nanoseconds), in [-1, 1].
func (n noise) value(t int64) float64 {
	return (math.Sin(n[0].angle(t)) + math.Sin(n[1].angle(t)) + math.Sin(n[2].angle(t))) / 3
}

// modelHash derives a hash for one component of a model from a series identity and an instance.
func modelHash(identity uint64, instanceID int, component uint64) uint64 {
	return splitmix64(splitmix64(identity^component*0x9e3779b97f4a7c15) ^ uint64(instanceID))
}

// splitmix64 is the splitmix64 finalizer.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
