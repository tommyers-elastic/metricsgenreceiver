package metricsgenreceiver

import (
	"math"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/distribution"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// instanceLifecycle holds the lifetime and restarts of an explicit instance, and the counter
// baseline of its current start.
//
// The baseline is an explicit, opt-in exception to keeping no per-series state: it exists only
// while an instance that started or restarted inside the generated window is alive, holds one
// value per counter series of the instance's template (see resetSlotsFor), is replaced on the
// next restart and is dropped once end_time has passed. To compute it, a template with such
// instances also keeps the value of each of its counter series before the latest advance
// (Scenario.previousResetValues): one value per series per template, not per instance.
type instanceLifecycle struct {
	// start and end bound the sample times the instance reports, [start, end); zero is unbounded.
	start time.Time
	end   time.Time
	// restarts are the restart times in ascending order.
	restarts []time.Time

	// baselineStart is the effective start the baseline belongs to; zero when there is none.
	baselineStart time.Time
	// variationStart is the time, relative to the ticks, from which the time-dependent counter
	// variation of the reset series runs: as far before the first tick emitted for baselineStart
	// as that tick's sample time is after baselineStart.
	variationStart time.Time
	// baseline holds the values subtracted from the template's reset series; nil when there is
	// none.
	baseline *counterBaseline
}

// counterBaseline holds the template values of a scenario's reset series, indexed by
// Scenario.resetSlots. Int and double values are kept apart so int values stay exact.
type counterBaseline struct {
	ints    []int64
	doubles []float64
}

// baselineSize is the number of int and double reset series of a template.
type baselineSize struct {
	ints    int
	doubles int
}

func newCounterBaseline(size baselineSize) *counterBaseline {
	return &counterBaseline{
		ints:    make([]int64, size.ints),
		doubles: make([]float64, size.doubles),
	}
}

func newInstanceLifecycle(c InstanceCfg) *instanceLifecycle {
	lifecycle := &instanceLifecycle{restarts: c.Restarts}
	if c.StartTime != nil {
		lifecycle.start = *c.StartTime
	}
	if c.EndTime != nil {
		lifecycle.end = *c.EndTime
	}
	return lifecycle
}

// active reports whether the instance reports a sample at sampleTime (tick plus interval
// offset, before jitter).
func (l *instanceLifecycle) active(sampleTime time.Time) bool {
	return !l.ended(sampleTime) && (l.start.IsZero() || !sampleTime.Before(l.start))
}

// ended reports whether end_time has passed at sampleTime.
func (l *instanceLifecycle) ended(sampleTime time.Time) bool {
	return !l.end.IsZero() && !sampleTime.Before(l.end)
}

// dropBaseline releases the counter baseline.
func (l *instanceLifecycle) dropBaseline() {
	l.baselineStart = time.Time{}
	l.baseline = nil
}

// effectiveStart returns the latest of the initial start timestamp and the restarts at or
// before sampleTime; zero if there is none.
func (l *instanceLifecycle) effectiveStart(initial time.Time, sampleTime time.Time) time.Time {
	start := initial
	for _, restart := range l.restarts {
		if restart.After(sampleTime) {
			break
		}
		start = restart
	}
	return start
}

// counterHintClasses are the hint classes, besides no hint, of the cumulative monotonic sums
// that a (re)start inside the window resets to zero. Sums with any other hint do not behave
// like counters and are not reset.
var counterHintClasses = map[distribution.GenerationHintClass]bool{
	distribution.GenerationHintSteadyCounter: true,
	distribution.GenerationHintSparseCounter: true,

	distribution.GenerationHintSeededRateCounter: true,
}

// resetsOnRestart reports whether a series of the metric counts from zero after a (re)start:
// it is a cumulative monotonic number sum without a hint or with a counter hint class.
func resetsOnRestart(m pmetric.Metric, hints map[string]distribution.MetricGenerationHint) bool {
	if !isCumulativeMonotonicSum(m) {
		return false
	}
	hint, hinted := hints[m.Name()]
	return !hinted || counterHintClasses[hint.Class]
}

// resetSlotsFor returns, for each data point of the template in ForEachDataPoint order, the
// index of its slot in the int or double values of a counter baseline (by the data point's
// value type) if the series resets on restart, or -1, along with the number of slots.
func resetSlotsFor(metrics *pmetric.Metrics, hints map[string]distribution.MetricGenerationHint) ([]int, baselineSize) {
	slots := make([]int, metrics.DataPointCount())
	var counts baselineSize
	dp.ForEachDataPoint(metrics, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		slots[idx] = -1
		numberDataPoint, isNumber := dataPoint.(pmetric.NumberDataPoint)
		if !isNumber || !resetsOnRestart(m, hints) {
			return
		}
		switch numberDataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			slots[idx] = counts.ints
			counts.ints++
		case pmetric.NumberDataPointValueTypeDouble:
			slots[idx] = counts.doubles
			counts.doubles++
		}
	})
	return slots, counts
}

func isCumulativeMonotonicSum(m pmetric.Metric) bool {
	return m.Type() == pmetric.MetricTypeSum &&
		m.Sum().IsMonotonic() &&
		m.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
}

// snapshotBaseline computes the baseline of a (re)start at effectiveStart on the first tick the
// instance emits for it, so that the instance's first sample reads what each reset series
// accumulated since effectiveStart at its rate over the latest interval:
//
//	baseline = V(tick) - (V(tick) - V(previous tick)) * (sampleTime - effectiveStart) / interval
//
// where V are the template values, rounded to a whole number for int values. The sample reads 0
// when sampleTime equals effectiveStart. A start more than one interval before the sample
// extrapolates the latest rate back to it; only absurd magnitudes are capped (see recordSince).
// The time-dependent counter variation starts at the same time before the tick.
func (l *instanceLifecycle) snapshotBaseline(scn *Scenario, effectiveStart, tick, sampleTime time.Time, interval time.Duration) {
	elapsed := sampleTime.Sub(effectiveStart)
	fraction := float64(elapsed) / float64(interval)
	baseline := newCounterBaseline(scn.resetSlotCounts)
	dp.ForEachDataPoint(scn.metricsTemplate, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
		if slot := scn.resetSlots[idx]; slot >= 0 {
			baseline.recordSince(dataPoint.(pmetric.NumberDataPoint), scn.previousResetValues, slot, fraction)
		}
	})
	l.baselineStart = effectiveStart
	l.variationStart = tick.Add(-elapsed)
	l.baseline = baseline
}

// record stores the value of v in the given slot.
func (b *counterBaseline) record(v pmetric.NumberDataPoint, slot int) {
	switch v.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		b.ints[slot] = v.IntValue()
	case pmetric.NumberDataPointValueTypeDouble:
		b.doubles[slot] = v.DoubleValue()
	}
}

// maxAccumulated caps what the first sample after a (re)start reads, before the per-instance
// variation: 2^62, far beyond any realistic counter. It is exact as a float64 and as an int64, so
// the int conversion is in range and identical on every architecture, and subtracting it from a
// finite double never overflows. For ints, the baseline and the later samples use saturating
// subtraction (see subtractInt), so the baseline can be negative without wrapping around.
const maxAccumulated = 1 << 62

// recordSince stores in the given slot the value of v minus what the series accumulated since
// the (re)start: fraction times its increase since the value in previous, at least 0 (NaN counts
// as 0) and at most maxAccumulated, rounded to a whole number for int values.
func (b *counterBaseline) recordSince(v pmetric.NumberDataPoint, previous *counterBaseline, slot int, fraction float64) {
	switch v.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		value := v.IntValue()
		increase := value - previous.ints[slot]
		b.ints[slot] = subtractInt(value, accumulatedInt(float64(float64(increase)*fraction)))
	case pmetric.NumberDataPointValueTypeDouble:
		value := v.DoubleValue()
		accumulated := float64((value - previous.doubles[slot]) * fraction)
		b.doubles[slot] = value - accumulatedDouble(accumulated)
	}
}

// accumulatedInt rounds accumulated to a whole number in [0, maxAccumulated]. NaN counts as 0.
func accumulatedInt(accumulated float64) int64 {
	return int64(math.Round(accumulatedDouble(accumulated)))
}

// accumulatedDouble clamps accumulated to [0, maxAccumulated]. NaN counts as 0.
func accumulatedDouble(accumulated float64) float64 {
	if !(accumulated > 0) {
		return 0
	}
	return math.Min(accumulated, maxAccumulated)
}

// subtractInt returns a - b, saturated to the int64 range instead of wrapping around.
func subtractInt(a, b int64) int64 {
	difference := a - b
	if (b > 0 && difference > a) || (b < 0 && difference < a) {
		if b > 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	return difference
}

// subtract sets v to its value minus the baseline in the given slot, clamped at 0 (and, for
// ints, saturated at the int64 maximum).
func (b *counterBaseline) subtract(v pmetric.NumberDataPoint, slot int) {
	switch v.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v.SetIntValue(max(0, subtractInt(v.IntValue(), b.ints[slot])))
	case pmetric.NumberDataPointValueTypeDouble:
		v.SetDoubleValue(max(0, v.DoubleValue()-b.doubles[slot]))
	}
}
