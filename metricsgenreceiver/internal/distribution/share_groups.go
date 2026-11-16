package distribution

import (
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// shareGroup is a group of series of one metric in one scope of the template that differ only in
// the attribute named by the share_by parameter. Its members' values add up to its total.
//
// The share of member s for an instance at time t is
//
//	f_s = seed_s * k_s / sum_j(seed_j * k_j)
//	share_s(t) = f_s * (1 + c * (n_s(t) - sum_j(f_j * n_j(t))))    c = min(variation, maxShareVariation) / 2
//
// where k_s is the member's per-instance factor and n_s its noise (see modelSeries). The shares
// add up to 1, and since n is in [-1, 1], share_s stays within f_s * (1 +- variation * (1 - f_s)),
// so it is never negative and a zero seed stays zero.
type shareGroup struct {
	class  GenerationHintClass
	metric string
	params *modelParams
	// members are the template data point indexes of the members, in template order.
	members []int
	// slot is the index of the first member's value in ModelScratch.values.
	slot int
	// total is what the members of a share_gauge group add up to.
	total     float64
	valueType pmetric.NumberDataPointValueType
	precision NumberPrecision
}

// ModelScratch holds the values of the share groups of a ModelPlan for the emission of one
// instance at one tick, so that a group is computed once for all its members. It is not safe for
// concurrent use; every emission gets its own from ModelPlan.GetScratch.
type ModelScratch struct {
	// values holds the members' values, at shareGroup.slot onwards for each group.
	values []float64
	// ready tells, for each group, whether values holds the group's values for the timestamp in
	// at.
	ready []bool
	at    []int64
	// work holds intermediate per-member values of the group being computed.
	work []float64
}

// GetScratch returns scratch space for emitting one instance at one tick, or nil when the plan
// has no share groups. Return it with PutScratch once the instance is emitted.
func (p *ModelPlan) GetScratch() *ModelScratch {
	if p == nil || len(p.groups) == 0 {
		return nil
	}
	scratch := p.scratch.Get().(*ModelScratch)
	clear(scratch.ready)
	return scratch
}

// PutScratch returns scratch space obtained from GetScratch.
func (p *ModelPlan) PutScratch(scratch *ModelScratch) {
	if scratch != nil {
		p.scratch.Put(scratch)
	}
}

func (p *ModelPlan) newScratch() *ModelScratch {
	return &ModelScratch{
		values: make([]float64, p.memberCount),
		ready:  make([]bool, len(p.groups)),
		at:     make([]int64, len(p.groups)),
		work:   make([]float64, p.maxGroupSize),
	}
}

// memberValue returns the value of the share group member s for the instance at t, computing
// the values of the whole group into scratch the first time one of its members is emitted at t.
func (p *ModelPlan) memberValue(s *modelSeries, instanceID int, t int64, scratch *ModelScratch) float64 {
	if scratch == nil {
		scratch = p.newScratch()
	}
	g := &p.groups[s.group]
	if !scratch.ready[s.group] || scratch.at[s.group] != t {
		values := scratch.values[g.slot : g.slot+len(g.members)]
		p.shares(g, instanceID, t, values, scratch.work[:len(g.members)])
		for i := range values {
			values[i] = float64(g.total * values[i])
		}
		g.roundToTotal(values)
		scratch.ready[s.group] = true
		scratch.at[s.group] = t
	}
	return scratch.values[g.slot+s.member]
}

// shares writes the share of every member of g for the instance at t into shares, using noises
// as scratch space.
func (p *ModelPlan) shares(g *shareGroup, instanceID int, t int64, shares []float64, noises []float64) {
	weightSum := 0.0
	for i, idx := range g.members {
		s := &p.series[idx]
		shares[i] = float64(s.seed * s.instanceFactor(instanceID))
		weightSum += shares[i]
		noises[i] = newNoise(s.identity, instanceID, g.params.period).value(t)
	}
	g.shareFormula(shares, noises, weightSum)
}

// shareFormula turns the members' weights (seed * k) in shares into their shares, given each
// member's noise and the sum of the weights.
func (g *shareGroup) shareFormula(shares []float64, noises []float64, weightSum float64) {
	if weightSum == 0 {
		clear(shares)
		return
	}
	mean := 0.0
	for i := range shares {
		shares[i] /= weightSum
		mean += float64(shares[i] * noises[i])
	}
	c := min(g.params.variation, maxShareVariation) / 2
	for i := range shares {
		shares[i] = float64(shares[i] * float64(1+float64(c*float64(noises[i]-mean))))
	}
}

// maxShareVariation caps the variation of the share classes just below 1, so that a member's
// share never falls below 1/1000 of its mean share f_s: with c = variation / 2 it stays at least
// f_s * (1 - maxShareVariation * (1 - f_s)) > f_s / 1000, where a variation of exactly 1 would
// let the share of a member with a tiny mean share f_s fall to about f_s^2.
const maxShareVariation = 0.999

// roundToTotal rounds every value to the precision of the group's values (whole numbers for
// ints, the inferred precision for doubles when one is set) and adds the rounding residual to
// the largest value, so the values add up to the rounded total. A value is never made negative:
// a residual larger than the largest value is spread over the next largest ones.
//
// For ints, the sum and the residual are computed in integer arithmetic: a float64 sum of whole
// numbers is not exact once a partial sum exceeds 2^53, even when the total does not. As the
// total of an int group is at most 2^53 (see maxExactIntTotal) and no value exceeds the total,
// every value converts to int64 exactly.
func (g *shareGroup) roundToTotal(values []float64) {
	sum := 0.0
	var intSum int64
	for i := range values {
		values[i] = roundModelValue(values[i], g.valueType, g.precision)
		sum += values[i]
		if g.valueType == pmetric.NumberDataPointValueTypeInt {
			intSum += int64(values[i])
		}
	}
	residual := roundModelValue(g.total, g.valueType, g.precision) - sum
	if g.valueType == pmetric.NumberDataPointValueTypeInt {
		residual = float64(int64(roundModelValue(g.total, g.valueType, g.precision)) - intSum)
	}
	for residual != 0 {
		largest := 0
		for i := range values {
			if values[i] > values[largest] {
				largest = i
			}
		}
		next := values[largest] + residual
		if next >= 0 {
			values[largest] = roundModelValue(next, g.valueType, g.precision)
			return
		}
		if values[largest] <= 0 {
			return
		}
		residual = next
		values[largest] = 0
	}
}

// shareGroupBuilder collects the share groups of a template.
type shareGroupBuilder struct {
	index  map[shareGroupKey]int
	groups []*pendingShareGroup
}

// shareGroupKey identifies a share group: its scope (the index of the scope among all scopes of
// the template), its metric and the canonical encoding of the attributes its members share (see
// appendCanonicalValue). As the encoding is unambiguous, equal keys mean equal attributes.
type shareGroupKey struct {
	scope      int
	metric     string
	attributes string
}

type pendingShareGroup struct {
	shareGroup
	// attributes describes the attributes the members share, for errors.
	attributes string
	// memberKeys holds the canonical encodings of the share_by values of the members.
	memberKeys map[string]bool
	seedSum    float64
	// intValueSum is the sum of the members' seed values for int groups, saturated just above
	// maxExactIntTotal.
	intValueSum int64
}

// maxExactIntTotal is the largest total an int share group may have: 2^53, up to which float64
// represents every whole number, so that the members of an int group add up exactly.
const maxExactIntTotal = 1 << 53

func newShareGroupBuilder() *shareGroupBuilder {
	return &shareGroupBuilder{index: map[shareGroupKey]int{}}
}

// add adds the data point v at template index idx, in the scope with index scope, to its share
// group, and sets the series' group and member.
func (b *shareGroupBuilder) add(series *modelSeries, idx int, scope int, m pmetric.Metric, v pmetric.NumberDataPoint) error {
	shareBy := series.params.shareBy
	member, ok := v.Attributes().Get(shareBy)
	if !ok {
		return fmt.Errorf("the data point with attributes %s has no attribute %q named by %s%s", formatAttributes(v.Attributes()), shareBy, HintMetadataPrefix, HintParamShareBy)
	}
	if series.seed < 0 {
		return fmt.Errorf("the data point with attributes %s has a negative value (%v)", formatAttributes(v.Attributes()), series.seed)
	}
	others := pcommon.NewMap()
	v.Attributes().CopyTo(others)
	others.Remove(shareBy)
	// A group never spans scopes: memberValue relies on all members of a group sharing their
	// timestamps, and the receiver's stagger_scopes offsets differ between scopes.
	key := shareGroupKey{scope: scope, metric: m.Name(), attributes: string(appendCanonicalMap(nil, others))}
	groupIndex, ok := b.index[key]
	if !ok {
		groupIndex = len(b.groups)
		b.index[key] = groupIndex
		b.groups = append(b.groups, &pendingShareGroup{
			shareGroup: shareGroup{
				class:     series.class,
				metric:    m.Name(),
				params:    series.params,
				valueType: v.ValueType(),
				precision: series.precision,
			},
			attributes: formatAttributes(others),
			memberKeys: map[string]bool{},
		})
	}
	g := b.groups[groupIndex]
	memberKey := string(appendCanonicalValue(nil, member))
	if g.memberKeys[memberKey] {
		return fmt.Errorf("more than one data point has the attributes %s", formatAttributes(v.Attributes()))
	}
	if v.ValueType() != g.valueType {
		return fmt.Errorf("the share group with attributes %s mixes int and double values", g.attributes)
	}
	g.memberKeys[memberKey] = true
	series.group = groupIndex
	series.member = len(g.members)
	g.members = append(g.members, idx)
	g.seedSum += series.seed
	if v.ValueType() == pmetric.NumberDataPointValueTypeInt {
		// Seeds are not negative, and both terms are at most maxExactIntTotal + 1: no overflow.
		g.intValueSum = min(g.intValueSum+min(v.IntValue(), maxExactIntTotal+1), maxExactIntTotal+1)
	}
	return nil
}

// build sets the plan's share groups.
func (b *shareGroupBuilder) build(plan *ModelPlan) error {
	slot := 0
	for _, g := range b.groups {
		group := g.shareGroup
		group.slot = slot
		group.total = g.seedSum
		if group.params.hasTotal {
			if group.params.total > 0 && g.seedSum == 0 {
				return fmt.Errorf("metric %q with generation hint %s: the share group with attributes %s has %s%s %v but all its values are 0", group.metric, group.class, g.attributes, HintMetadataPrefix, HintParamTotal, group.params.total)
			}
			group.total = group.params.total
		}
		if group.valueType == pmetric.NumberDataPointValueTypeInt {
			exceeds := g.intValueSum > maxExactIntTotal
			if group.params.hasTotal {
				exceeds = group.params.total > maxExactIntTotal
			}
			if exceeds {
				return fmt.Errorf("metric %q with generation hint %s: the int share group with attributes %s adds up to more than 2^53 (%d), the largest total up to which int values add up exactly", group.metric, group.class, g.attributes, int64(maxExactIntTotal))
			}
		}
		plan.groups = append(plan.groups, group)
		slot += len(group.members)
		plan.maxGroupSize = max(plan.maxGroupSize, len(group.members))
	}
	plan.memberCount = slot
	return nil
}

// formatAttributes formats attributes as {key=value, ...} in key order.
func formatAttributes(attrs pcommon.Map) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, key := range sortedMapKeys(attrs) {
		if i > 0 {
			b.WriteString(", ")
		}
		value, _ := attrs.Get(key)
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(value.AsString())
	}
	b.WriteByte('}')
	return b.String()
}

// appendCanonicalMap appends an unambiguous encoding of attrs to b: the number of entries, then
// each key and value in key order (see appendCanonicalValue). Unlike appendAttributesKey, which
// feeds the identity hashes and must not change, no attribute key or value can be mistaken for
// the separators around it, so two maps have the same encoding only if they are equal.
func appendCanonicalMap(b []byte, attrs pcommon.Map) []byte {
	b = append(b, 'm')
	b = strconv.AppendInt(b, int64(attrs.Len()), 10)
	b = append(b, ':')
	for _, key := range sortedMapKeys(attrs) {
		value, _ := attrs.Get(key)
		b = appendCanonicalString(b, key)
		b = appendCanonicalValue(b, value)
	}
	return b
}

// appendCanonicalValue appends an unambiguous encoding of value to b: a type tag followed by
// the length of strings, byte strings and collections, or by the value terminated by ';'.
func appendCanonicalValue(b []byte, value pcommon.Value) []byte {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return appendCanonicalString(b, value.Str())
	case pcommon.ValueTypeInt:
		b = append(b, 'i')
		b = strconv.AppendInt(b, value.Int(), 10)
		return append(b, ';')
	case pcommon.ValueTypeDouble:
		b = append(b, 'd')
		b = strconv.AppendFloat(b, value.Double(), 'g', -1, 64)
		return append(b, ';')
	case pcommon.ValueTypeBool:
		b = append(b, 'b')
		b = strconv.AppendBool(b, value.Bool())
		return append(b, ';')
	case pcommon.ValueTypeBytes:
		raw := value.Bytes().AsRaw()
		b = append(b, 'y')
		b = strconv.AppendInt(b, int64(len(raw)), 10)
		b = append(b, ':')
		return append(b, raw...)
	case pcommon.ValueTypeMap:
		return appendCanonicalMap(b, value.Map())
	case pcommon.ValueTypeSlice:
		values := value.Slice()
		b = append(b, 'l')
		b = strconv.AppendInt(b, int64(values.Len()), 10)
		b = append(b, ':')
		for i := 0; i < values.Len(); i++ {
			b = appendCanonicalValue(b, values.At(i))
		}
		return b
	default:
		return append(b, 'e')
	}
}

func appendCanonicalString(b []byte, s string) []byte {
	b = append(b, 's')
	b = strconv.AppendInt(b, int64(len(s)), 10)
	b = append(b, ':')
	return append(b, s...)
}
