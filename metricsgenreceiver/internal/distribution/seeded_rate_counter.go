package distribution

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	// seededRateNoiseStdDev is the standard deviation of the relative noise on each increase.
	seededRateNoiseStdDev = 0.1
	// minSeededRateMultiplier is the lower bound of the per-instance multiplier, which lies in
	// [minSeededRateMultiplier, minSeededRateMultiplier+1).
	minSeededRateMultiplier = 0.5
)

// SeededRates returns, for each data point of the template in ForEachDataPoint order, the
// per-second rate of the series whose metric is hinted GenerationHintSeededRateCounter: the
// value of the data point divided by the time between its start timestamp and its timestamp.
// Other data points get 0. It returns nil when no metric uses the hint. It must be called before
// the template's timestamps are overwritten.
func SeededRates(metrics *pmetric.Metrics, hints map[string]MetricGenerationHint) ([]float64, error) {
	used := false
	for _, hint := range hints {
		used = used || hint.Class == GenerationHintSeededRateCounter
	}
	if !used {
		return nil, nil
	}
	rates := make([]float64, metrics.DataPointCount())
	var errs []error
	dp.ForEachDataPoint(metrics, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		hint, ok := hints[m.Name()]
		if !ok || hint.Class != GenerationHintSeededRateCounter {
			return
		}
		numberDataPoint, ok := dataPoint.(pmetric.NumberDataPoint)
		if !ok {
			return
		}
		if m.Type() != pmetric.MetricTypeSum {
			errs = append(errs, fmt.Errorf("metric %q with generation hint %s: the metric must be a sum", m.Name(), GenerationHintSeededRateCounter))
			return
		}
		rate, err := seededRate(numberDataPoint)
		if err != nil {
			errs = append(errs, fmt.Errorf("metric %q with generation hint %s: %w", m.Name(), GenerationHintSeededRateCounter, err))
			return
		}
		rates[idx] = rate
	})
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return rates, nil
}

func seededRate(v pmetric.NumberDataPoint) (float64, error) {
	start, end := v.StartTimestamp(), v.Timestamp()
	if start == 0 || end == 0 {
		return 0, errors.New("the data point needs both startTimeUnixNano and timeUnixNano")
	}
	if end <= start {
		return 0, fmt.Errorf("timeUnixNano (%d) must be after startTimeUnixNano (%d)", end, start)
	}
	value := currentNumberDataPointValue(v)
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, fmt.Errorf("the value (%v) must be a finite number that is not negative", value)
	}
	return value / time.Duration(end-start).Seconds(), nil
}

type seededRateCounterStrategy struct{}

// Advance increases the value by the series' rate over one interval, with a small relative
// noise, rounded probabilistically to the precision the value is kept at so that increases
// smaller than that precision are not lost.
func (seededRateCounterStrategy) Advance(current float64, rand *rand.Rand, metric pmetric.Metric, _ DistributionCfg, opts AdvanceOptions) float64 {
	noise := math.Max(0, 1+float64(seededRateNoiseStdDev*rand.NormFloat64()))
	delta := float64(opts.SeriesRate * opts.Interval.Seconds() * noise)
	delta = roundProbabilistically(delta, unitsPerQuantum(metric, opts), rand)
	return applyCounterDelta(metric, current, delta)
}

// unitsPerQuantum returns the inverse of the smallest step the value is kept at: 1 for int
// values, 10^precision for double values with an inferred precision, and 0 (no rounding)
// otherwise.
func unitsPerQuantum(metric pmetric.Metric, opts AdvanceOptions) float64 {
	switch opts.ValueType {
	case pmetric.NumberDataPointValueTypeInt:
		return 1
	case pmetric.NumberDataPointValueTypeDouble:
		if p, ok := opts.Precision[metric.Name()]; ok {
			return math.Pow(10, float64(p))
		}
	}
	return 0
}

// roundProbabilistically rounds v down to a whole number of quanta (1/unitsPerQuantum) and adds
// one more quantum with a probability equal to the remainder, so the expected value is v. It
// draws once from rand unless unitsPerQuantum is 0, in which case v is returned as is.
func roundProbabilistically(v float64, unitsPerQuantum float64, rand *rand.Rand) float64 {
	if unitsPerQuantum == 0 {
		return v
	}
	units := float64(v * unitsPerQuantum)
	whole := math.Floor(units)
	if rand.Float64() < units-whole {
		whole++
	}
	return whole / unitsPerQuantum
}

// seededRateVariation multiplies the value by a stable per-instance factor in [0.5, 1.5), so
// each instance's rate is a fixed multiple of the template's, cumulative series stay monotonic
// and zero stays zero.
type seededRateVariation struct{}

func (seededRateVariation) ApplyNumber(v pmetric.NumberDataPoint, instanceID int, identityHash uint64, _ pmetric.Metric, precision NumberPrecision, _ InstanceVariationOptions) {
	multiplier := minSeededRateMultiplier + float64(hashToUnitInterval(mixInstanceID(identityHash, instanceID)))
	writeNumberDataPoint(v, float64(currentNumberDataPointValue(v)*multiplier), precision)
}
