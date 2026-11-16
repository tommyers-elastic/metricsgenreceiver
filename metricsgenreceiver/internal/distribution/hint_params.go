package distribution

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

// HintMetadataPrefix is the prefix of the metric metadata keys that declare a generation hint:
// the class is declared under HintMetadataPrefix + "class" and, for a class that takes
// parameters, each parameter under HintMetadataPrefix + its name.
const HintMetadataPrefix = "metricsgen.hint."

// Hint parameter names, declared in metric metadata under HintMetadataPrefix.
const (
	HintParamVariation      = "variation"
	HintParamPeriod         = "period"
	HintParamInstanceSpread = "instance_spread"
	HintParamMin            = "min"
	HintParamMax            = "max"
	HintParamVariationKey   = "variation_key"
	HintParamShareBy        = "share_by"
	HintParamTotal          = "total"
)

const (
	defaultModelVariation      = 0.25
	defaultModelPeriod         = 10 * time.Minute
	defaultModelInstanceSpread = 2.0
	// maxModelPeriod bounds the period so that the periods of the noise components fit in
	// nanoseconds.
	maxModelPeriod = 365 * 24 * time.Hour
)

// TakesParams reports whether the hint class takes parameters. For such a class every metadata
// key under HintMetadataPrefix is a parameter, read and validated strictly. For any other class
// only the class key is read, and other keys under the prefix are not hint metadata.
func (c GenerationHintClass) TakesParams() bool {
	_, ok := classParams[c]
	return ok
}

// classParams lists the parameters each hint class accepts. It is the single source of truth for
// which classes take parameters: a class takes parameters iff it is listed (see TakesParams).
var classParams = map[GenerationHintClass][]string{
	GenerationHintBoundedGauge: {HintParamVariation, HintParamPeriod, HintParamInstanceSpread, HintParamMin, HintParamMax, HintParamVariationKey},
	GenerationHintShareGauge:   {HintParamShareBy, HintParamTotal, HintParamVariation, HintParamPeriod, HintParamInstanceSpread, HintParamVariationKey},
	GenerationHintShareCounter: {HintParamShareBy, HintParamVariation, HintParamPeriod, HintParamInstanceSpread, HintParamVariationKey},
}

// requiredParams lists the parameters a hint class requires.
var requiredParams = map[GenerationHintClass][]string{
	GenerationHintShareGauge:   {HintParamShareBy},
	GenerationHintShareCounter: {HintParamShareBy},
}

// modelParams are the parsed parameters of a stateless value model hint class.
type modelParams struct {
	variation      float64
	period         time.Duration
	instanceSpread float64
	min            float64
	max            float64
	hasMin         bool
	hasMax         bool
	variationKey   string
	shareBy        string
	total          float64
	hasTotal       bool
}

// parseParams validates the hint's parameters for its class and returns them with the defaults
// applied. Errors name the metric and the parameter.
func (h MetricGenerationHint) parseParams(metricName string) (modelParams, error) {
	params := modelParams{
		variation:      defaultModelVariation,
		period:         defaultModelPeriod,
		instanceSpread: defaultModelInstanceSpread,
	}
	allowed := map[string]bool{}
	for _, name := range classParams[h.Class] {
		allowed[name] = true
	}
	// Report the parameters in a stable order.
	names := make([]string, 0, len(h.Params))
	for name := range h.Params {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := h.Params[name]
		if !allowed[name] {
			return modelParams{}, fmt.Errorf("metric %q with generation hint %s: unknown parameter %s%s", metricName, h.Class, HintMetadataPrefix, name)
		}
		invalid := func(want string) error {
			return fmt.Errorf("metric %q with generation hint %s: parameter %s%s must be %s, got %q", metricName, h.Class, HintMetadataPrefix, name, want, value)
		}
		switch name {
		case HintParamVariation:
			v, ok := parseFiniteNumber(value)
			if !ok || v < 0 || v > 1 {
				return modelParams{}, invalid("a number in [0, 1]")
			}
			params.variation = v
		case HintParamPeriod:
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 || d > maxModelPeriod {
				return modelParams{}, invalid("a positive duration of at most " + maxModelPeriod.String())
			}
			params.period = d
		case HintParamInstanceSpread:
			v, ok := parseFiniteNumber(value)
			if !ok || v < 1 {
				return modelParams{}, invalid("a number >= 1")
			}
			params.instanceSpread = v
		case HintParamMin:
			v, ok := parseFiniteNumber(value)
			if !ok {
				return modelParams{}, invalid("a number")
			}
			params.min, params.hasMin = v, true
		case HintParamMax:
			v, ok := parseFiniteNumber(value)
			if !ok {
				return modelParams{}, invalid("a number")
			}
			params.max, params.hasMax = v, true
		case HintParamVariationKey:
			if value == "" {
				return modelParams{}, invalid("a non-empty string")
			}
			params.variationKey = value
		case HintParamShareBy:
			if value == "" {
				return modelParams{}, invalid("a non-empty attribute name")
			}
			params.shareBy = value
		case HintParamTotal:
			v, ok := parseFiniteNumber(value)
			if !ok || v < 0 {
				return modelParams{}, invalid("a number >= 0")
			}
			params.total, params.hasTotal = v, true
		}
	}
	for _, name := range requiredParams[h.Class] {
		if _, ok := h.Params[name]; !ok {
			return modelParams{}, fmt.Errorf("metric %q with generation hint %s: parameter %s%s is required", metricName, h.Class, HintMetadataPrefix, name)
		}
	}
	if params.hasMin && params.hasMax && params.min > params.max {
		return modelParams{}, fmt.Errorf("metric %q with generation hint %s: parameter %s%s (%v) must not be greater than %s%s (%v)", metricName, h.Class, HintMetadataPrefix, HintParamMin, params.min, HintMetadataPrefix, HintParamMax, params.max)
	}
	return params, nil
}

func parseFiniteNumber(s string) (float64, bool) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}

// checkVariationKeys checks that the model metrics with the same variation key (variation_key,
// or the metric name by default) agree on the parameters that decide whether their series vary
// together. For every model class, these are period and instance_spread, which set the noise and
// the per-instance factor; bounded_gauge metrics may differ in variation, which only scales the
// noise. Share class metrics must in addition agree on share_by and variation, which set the
// groups and the share formula. A mismatch is an error naming both metrics.
func checkVariationKeys(hints map[string]MetricGenerationHint, params map[string]*modelParams) error {
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	// Report the first mismatch in a stable order.
	sort.Strings(names)
	first := map[string]string{}
	firstShare := map[string]string{}
	for _, name := range names {
		key := params[name].effectiveVariationKey(name)
		if other, ok := first[key]; ok {
			if err := compareKeyedParams(key, other, params[other], name, params[name], modelKeyedParams); err != nil {
				return err
			}
		} else {
			first[key] = name
		}
		if !shareClasses[hints[name].Class] {
			continue
		}
		if other, ok := firstShare[key]; ok {
			if err := compareKeyedParams(key, other, params[other], name, params[name], shareKeyedParams); err != nil {
				return err
			}
		} else {
			firstShare[key] = name
		}
	}
	return nil
}

// effectiveVariationKey returns the variation_key, or the metric name when it is not set.
func (p *modelParams) effectiveVariationKey(metricName string) string {
	if p.variationKey != "" {
		return p.variationKey
	}
	return metricName
}

// keyedParam is a parameter that metrics with the same variation key must agree on.
type keyedParam struct {
	name  string
	value func(p *modelParams) string
}

// modelKeyedParams are the parameters every model metric with the same variation key must agree
// on.
var modelKeyedParams = []keyedParam{
	{HintParamPeriod, func(p *modelParams) string { return p.period.String() }},
	{HintParamInstanceSpread, func(p *modelParams) string { return strconv.FormatFloat(p.instanceSpread, 'g', -1, 64) }},
}

// shareKeyedParams are the further parameters share class metrics with the same variation key
// must agree on.
var shareKeyedParams = []keyedParam{
	{HintParamShareBy, func(p *modelParams) string { return p.shareBy }},
	{HintParamVariation, func(p *modelParams) string { return strconv.FormatFloat(p.variation, 'g', -1, 64) }},
}

func compareKeyedParams(key string, nameA string, a *modelParams, nameB string, b *modelParams, keyed []keyedParam) error {
	for _, param := range keyed {
		if valueA, valueB := param.value(a), param.value(b); valueA != valueB {
			return fmt.Errorf("metrics %q and %q have the same variation key %q (%s%s, or the metric name) but different %s%s (%q and %q): their series only vary together when it is the same", nameA, nameB, key, HintMetadataPrefix, HintParamVariationKey, HintMetadataPrefix, param.name, valueA, valueB)
		}
	}
	return nil
}
