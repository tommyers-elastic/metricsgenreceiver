package metricsgenreceiver

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
	"math/rand"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Explicitly declared instances do not draw from the receiver's shared random number generator.
// Everything random about such an instance is derived from its key, a hash of the parts of the
// configuration that define it, so its output does not depend on its position in the list, on
// other instances or on other scenarios:
//
//   - the shared template of a group of instances with equal template variables advances with
//     its own generator, seeded from the seed, the scenario path and the variables;
//   - the resource attributes template of an instance is rendered with a generator seeded from
//     its key;
//   - the jitter and the explicit-bucket histogram values of an instance are drawn, at every
//     tick, from a generator reseeded from its key and the tick;
//   - the per-instance variation and the stagger_instances offset are derived from its key.

// Domains separate the streams derived from one key.
const (
	groupStreamDomain    uint64 = 0x6a09e667f3bcc908
	resourceStreamDomain uint64 = 0xbb67ae8584caa73b
	emissionStreamDomain uint64 = 0x3c6ef372fe94f82b
	staggerDomain        uint64 = 0xa54ff53a5f1d36f1
)

// splitMix64 is a rand.Source64 with eight bytes of state. Every explicit instance group gets its
// own generator, and a math/rand source from rand.NewSource takes about 5 KB, so a small source
// keeps memory low with many groups. Seeding is a single assignment, so the generator can be
// reseeded cheaply for every instance and tick.
type splitMix64 struct {
	state uint64
}

func newSplitMix64Rand(seed uint64) *rand.Rand {
	return rand.New(&splitMix64{state: seed})
}

func (s *splitMix64) Uint64() uint64 {
	s.state += 0x9e3779b97f4a7c15
	z := s.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (s *splitMix64) Int63() int64 {
	return int64(s.Uint64() >> 1)
}

func (s *splitMix64) Seed(seed int64) {
	s.state = uint64(seed)
}

// reseed restarts r, which must use a splitMix64 source, at the given state. rand.Rand.Seed also
// discards bytes buffered by a previous Read.
func reseed(r *rand.Rand, state uint64) {
	r.Seed(int64(state))
}

// explicitScenarioHash hashes the seed and the scenario path, the parts of an explicit instance's
// key that all instances of a scenario have in common.
func explicitScenarioHash(seed int64, path string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	return mix64(mix64(uint64(seed)) ^ h.Sum64())
}

// templateVarsHash returns the hash of an instance group: the scenario hash combined with the
// digest of the canonical JSON encoding of the group's merged template variables.
func templateVarsHash(scenarioHash, varsDigest uint64) uint64 {
	return mix64(scenarioHash ^ varsDigest)
}

// groupStreamSeed returns the seed of the generator that advances a group's shared template.
func groupStreamSeed(groupHash uint64) uint64 {
	return mix64(groupHash ^ groupStreamDomain)
}

// explicitInstanceKey returns the key of an explicit instance from the hash of its group and the
// digest of its resource attributes. duplicate is the number of earlier instances of the scenario
// with the same group hash and resource attributes; it is mixed in so that repeated definitions
// still differ.
func explicitInstanceKey(groupHash, attributesDigest uint64, duplicate int) uint64 {
	key := mix64(groupHash ^ mix64(attributesDigest))
	if duplicate > 0 {
		key = mix64(key ^ uint64(duplicate))
	}
	return key
}

// variationID returns the non-negative instance ID that drives the per-instance variation of an
// explicit instance.
func variationID(key uint64) int {
	return int(key & uint64(math.MaxInt))
}

// resourceStreamSeed returns the seed of the generator that renders an explicit instance's
// resource attributes template.
func resourceStreamSeed(key uint64) uint64 {
	return mix64(key ^ resourceStreamDomain)
}

// emissionStreamSeed returns the seed of the generator for an explicit instance's draws at the
// given tick (jitter and explicit-bucket histograms).
func emissionStreamSeed(key uint64, tick time.Time) uint64 {
	return mix64(mix64(key^emissionStreamDomain) ^ uint64(tick.UnixNano()))
}

// staggerOffsetForKey returns the stagger_instances offset of an explicit instance, or 0 when
// staggering is off. Integer arithmetic only, like staggerOffset.
func staggerOffsetForKey(cfg *Config, key uint64) time.Duration {
	if !cfg.StaggerInstances || cfg.Interval <= 0 {
		return 0
	}
	return time.Duration(mix64(key^staggerDomain) % uint64(cfg.Interval))
}

// attributesDigest returns a hash of a resource attribute map. The encoding tags every value with
// its type and prefixes strings and collections with their length, so different maps (including
// 1 and 1.0) hash differently. Keys are hashed in iteration order, which is sorted for maps built
// by InstanceCfg.resourceAttributes.
func attributesDigest(attrs pcommon.Map) uint64 {
	h := fnv.New64a()
	var buf [9]byte
	writeMap(h, &buf, attrs)
	return h.Sum64()
}

func writeMap(h hash.Hash64, buf *[9]byte, m pcommon.Map) {
	writeTagged(h, buf, 'm', uint64(m.Len()))
	m.Range(func(k string, v pcommon.Value) bool {
		writeString(h, buf, k)
		writeValue(h, buf, v)
		return true
	})
}

func writeValue(h hash.Hash64, buf *[9]byte, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		writeString(h, buf, v.Str())
	case pcommon.ValueTypeInt:
		writeTagged(h, buf, 'i', uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		writeTagged(h, buf, 'd', math.Float64bits(v.Double()))
	case pcommon.ValueTypeBool:
		b := uint64(0)
		if v.Bool() {
			b = 1
		}
		writeTagged(h, buf, 'b', b)
	case pcommon.ValueTypeBytes:
		raw := v.Bytes().AsRaw()
		writeTagged(h, buf, 'y', uint64(len(raw)))
		_, _ = h.Write(raw)
	case pcommon.ValueTypeMap:
		writeMap(h, buf, v.Map())
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		writeTagged(h, buf, 'l', uint64(s.Len()))
		for i := 0; i < s.Len(); i++ {
			writeValue(h, buf, s.At(i))
		}
	default:
		writeTagged(h, buf, 'e', 0)
	}
}

func writeString(h hash.Hash64, buf *[9]byte, s string) {
	writeTagged(h, buf, 's', uint64(len(s)))
	_, _ = h.Write([]byte(s))
}

func writeTagged(h hash.Hash64, buf *[9]byte, tag byte, v uint64) {
	buf[0] = tag
	binary.LittleEndian.PutUint64(buf[1:], v)
	_, _ = h.Write(buf[:])
}
