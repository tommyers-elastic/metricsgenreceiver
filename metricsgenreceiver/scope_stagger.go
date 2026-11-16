package metricsgenreceiver

import (
	"hash/fnv"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// With stagger_scopes, every instrumentation scope of an instance reports at its own fixed offset
// in [0, stagger_scopes) after the instance's timestamp (tick, interval offset and jitter): a
// simple model of a collector that runs one scraper per scope, whose scrapers report within a few
// milliseconds of each other in a stable order. The offset is a hash of the instance and of the
// scope's name and version, so it is fixed for every (instance, scope) pair and draws nothing from
// the random number generators. The per-scope part of the hash is computed once per template
// (seriesScopeHashes) and the per-instance part once per emission (instanceScopeHash).

// scopeStaggerDomain separates the scope offsets from the other streams derived from an explicit
// instance's key; see instance_streams.go.
const scopeStaggerDomain uint64 = 0x510e527fade682d1

// seriesScopeHashes returns, for each data point of the template in ForEachDataPoint order, the
// hash of the name and version of its instrumentation scope.
func seriesScopeHashes(metrics *pmetric.Metrics) []uint64 {
	hashes := make([]uint64, metrics.DataPointCount())
	dp.ForEachDataPoint(metrics, func(idx int, _ pcommon.Resource, is pcommon.InstrumentationScope, _ pmetric.Metric, _ dp.DataPoint) {
		hashes[idx] = scopeHash(is)
	})
	return hashes
}

// scopeHash hashes the name and the version of an instrumentation scope.
func scopeHash(is pcommon.InstrumentationScope) uint64 {
	h := fnv.New64a()
	var buf [9]byte
	writeString(h, &buf, is.Name())
	writeString(h, &buf, is.Version())
	return h.Sum64()
}

// instanceScopeHash returns the per-instance part of the scope offsets of an instance: derived
// from its key for an explicit instance, and from the seed, the scenario's position in the
// configuration and the instance ID for a generated one.
func instanceScopeHash(cfg *Config, scn *Scenario, instance *scenarioInstance) uint64 {
	if scn.explicitInstances {
		return mix64(instance.key ^ scopeStaggerDomain)
	}
	return mix64(generatedInstanceHash(cfg.Seed, scn.configIndex, instance.id) ^ scopeStaggerDomain)
}

// scopeOffset returns the offset of the scope with the given hash for the instance with the given
// instanceScopeHash, in [0, staggerScopes). staggerScopes must be positive. Integer arithmetic
// only, so it is the same on every architecture.
func scopeOffset(instanceHash, scopeHash uint64, staggerScopes time.Duration) time.Duration {
	return time.Duration(mix64(instanceHash^scopeHash) % uint64(staggerScopes))
}
