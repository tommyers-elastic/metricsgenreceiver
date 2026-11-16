package metricsgenreceiver

import (
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
// next restart and is dropped once end_time has passed.
type instanceLifecycle struct {
	// start and end bound the sample times the instance reports, [start, end); zero is unbounded.
	start time.Time
	end   time.Time
	// restarts are the restart times in ascending order.
	restarts []time.Time

	// baselineStart is the effective start the baseline belongs to; zero when there is none.
	baselineStart time.Time
	// baselineTick is the first tick the instance emitted for baselineStart.
	baselineTick time.Time
	// baseline holds the template values of the scenario's reset series at baselineTick; nil
	// when there is none.
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

// snapshotBaseline records the current template values of the scenario's reset series.
func (l *instanceLifecycle) snapshotBaseline(scn *Scenario, effectiveStart, tick time.Time) {
	baseline := &counterBaseline{
		ints:    make([]int64, scn.resetSlotCounts.ints),
		doubles: make([]float64, scn.resetSlotCounts.doubles),
	}
	dp.ForEachDataPoint(scn.metricsTemplate, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, _ pmetric.Metric, dataPoint dp.DataPoint) {
		if slot := scn.resetSlots[idx]; slot >= 0 {
			baseline.record(dataPoint.(pmetric.NumberDataPoint), slot)
		}
	})
	l.baselineStart = effectiveStart
	l.baselineTick = tick
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

// subtract sets v to its value minus the baseline in the given slot, clamped at 0.
func (b *counterBaseline) subtract(v pmetric.NumberDataPoint, slot int) {
	switch v.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v.SetIntValue(max(0, v.IntValue()-b.ints[slot]))
	case pmetric.NumberDataPointValueTypeDouble:
		v.SetDoubleValue(max(0, v.DoubleValue()-b.doubles[slot]))
	}
}
