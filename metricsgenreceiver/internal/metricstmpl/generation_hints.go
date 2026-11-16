package metricstmpl

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/distribution"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// GenerationHintMetadataKey is the per-metric metadata key used to declare a generation hint
// inline in the template. Values are the string form of a distribution.GenerationHintClass
// (e.g. "steady_counter"). The receiver reads and validates this key at startup, removes it
// from the template so it does not leak into emitted data, and falls back to default evolution
// for any metric that does not declare one.
//
// For a class that takes parameters (see distribution.GenerationHintClass.TakesParams), every
// further key with the same prefix (distribution.HintMetadataPrefix followed by the parameter
// name, such as "metricsgen.hint.variation") is a parameter with a string value. Such keys are
// validated for the class and stripped like the class. For any other class, and for a metric
// without a class, only this key is read: other keys under the prefix are left as they are and
// passed through to the emitted data.
const GenerationHintMetadataKey = distribution.HintMetadataPrefix + "class"

type metricHintConsistency struct {
	hint    distribution.MetricGenerationHint
	hasHint bool
	seen    bool
}

// ExtractGenerationHints reads GenerationHintMetadataKey from each metric's metadata, and the
// hint parameters if the class takes parameters, validates them, strips the hint keys so they do
// not propagate to emitted data, and returns a metric-name -> hint map. Hints are enforced per
// metric family within a scenario, so every occurrence of the same metric name must either
// declare the same class (and, for a class that takes parameters, the same parameters) or declare
// no hint at all. If any metric declares an invalid hint, all validation errors are joined and
// returned; the template is not mutated in that case.
func ExtractGenerationHints(metrics *pmetric.Metrics) (map[string]distribution.MetricGenerationHint, error) {
	hints := make(map[string]distribution.MetricGenerationHint)
	consistency := make(map[string]metricHintConsistency)
	var errs []error
	dp.ForEachMetric(metrics, func(_ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric) {
		v, ok := m.Metadata().Get(GenerationHintMetadataKey)
		state := consistency[m.Name()]
		if !ok {
			if state.hasHint {
				errs = append(errs, fmt.Errorf("metric %q must declare %s consistently across all occurrences", m.Name(), GenerationHintMetadataKey))
			}
			state.seen = true
			consistency[m.Name()] = state
			return
		}
		if !supportsGenerationHints(m) {
			errs = append(errs, fmt.Errorf("metric %q with %s must be a gauge or sum", m.Name(), GenerationHintMetadataKey))
			return
		}
		if v.Type() != pcommon.ValueTypeStr {
			errs = append(errs, fmt.Errorf("metric %q has non-string %s metadata", m.Name(), GenerationHintMetadataKey))
			return
		}
		hint := distribution.MetricGenerationHint{Class: distribution.GenerationHintClass(v.Str())}
		if hint.Class.TakesParams() {
			params, err := hintParams(m)
			if err != nil {
				errs = append(errs, err)
				return
			}
			hint.Params = params
		}
		if err := hint.Validate(m.Name()); err != nil {
			errs = append(errs, err)
			return
		}
		if state.seen && !state.hasHint {
			errs = append(errs, fmt.Errorf("metric %q must declare %s consistently across all occurrences", m.Name(), GenerationHintMetadataKey))
			return
		}
		if state.hasHint && state.hint.Class != hint.Class {
			errs = append(errs, fmt.Errorf("metric %q must use the same %s across all occurrences", m.Name(), GenerationHintMetadataKey))
			return
		}
		if state.hasHint && !maps.Equal(state.hint.Params, hint.Params) {
			errs = append(errs, fmt.Errorf("metric %q must declare the same hint parameters across all occurrences", m.Name()))
			return
		}
		state.seen = true
		state.hasHint = true
		state.hint = hint
		consistency[m.Name()] = state
		hints[m.Name()] = hint
	})
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	// Only strip on success, so the template stays inspectable when reporting validation errors.
	// On success, every occurrence of a metric name in hints declares its class.
	dp.ForEachMetric(metrics, func(_ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric) {
		hint, ok := hints[m.Name()]
		if !ok || !hint.Class.TakesParams() {
			m.Metadata().Remove(GenerationHintMetadataKey)
			return
		}
		m.Metadata().RemoveIf(func(key string, _ pcommon.Value) bool {
			return strings.HasPrefix(key, distribution.HintMetadataPrefix)
		})
	})
	return hints, nil
}

// hintParams returns the hint parameters declared in the metric's metadata by name, or nil if
// there are none. It is only called for a class that takes parameters.
func hintParams(m pmetric.Metric) (map[string]string, error) {
	var params map[string]string
	var err error
	m.Metadata().Range(func(key string, value pcommon.Value) bool {
		name, isHint := strings.CutPrefix(key, distribution.HintMetadataPrefix)
		if !isHint || key == GenerationHintMetadataKey {
			return true
		}
		if value.Type() != pcommon.ValueTypeStr {
			err = fmt.Errorf("metric %q has non-string %s metadata", m.Name(), key)
			return false
		}
		if params == nil {
			params = map[string]string{}
		}
		params[name] = value.Str()
		return true
	})
	return params, err
}

func supportsGenerationHints(m pmetric.Metric) bool {
	return m.Type() == pmetric.MetricTypeGauge || m.Type() == pmetric.MetricTypeSum
}
