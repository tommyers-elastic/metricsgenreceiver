package metricsgenreceiver

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// hintPassthroughSHA256 is the SHA-256 of the output of TestHintMetadataPassthrough as produced
// by the code before hint parameters existed, when only metricsgen.hint.class was read and
// stripped (the same on arm64 and amd64). Classes that take no parameters, and metrics without a
// class, must keep producing exactly that output.
const hintPassthroughSHA256 = "dc633d8eb7cef772492e264e8a6cb5055fba0575cc25eded34727b118f381585"

// TestHintMetadataPassthrough checks that for classes that take no parameters, and for metrics
// without a class, metricsgen.hint.* metadata other than the class is passed through unchanged.
func TestHintMetadataPassthrough(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.StartTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg.Interval = 10 * time.Second
	cfg.EndTime = cfg.StartTime.Add(5 * cfg.Interval)
	cfg.Seed = 3
	cfg.Scenarios = []ScenarioCfg{{Path: "testdata/hint-passthrough-template", Scale: 2}}
	require.NoError(t, cfg.Validate())

	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 10)
	wantMetadata := map[string]map[string]any{
		"system.memory.limit":        {"metricsgen.hint.foo": "kept", "metricsgen.hint.bar": int64(7)},
		"system.network.up":          {"metricsgen.hint.variation": "0.5"},
		"system.network.connections": {},
		"system.processes.count":     {"metricsgen.hint.baz": "unhinted"},
	}
	for _, md := range allMetrics {
		dp.ForEachMetric(&md, func(_ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric) {
			want, ok := wantMetadata[m.Name()]
			require.True(t, ok, m.Name())
			assert.Equal(t, want, m.Metadata().AsRaw(), m.Name())
			_, hasClass := m.Metadata().Get("metricsgen.hint.class")
			assert.False(t, hasClass, m.Name())
		})
	}

	sum := sha256.Sum256(marshalMetricsOutput(t, allMetrics).Bytes())
	assert.Equal(t, hintPassthroughSHA256, hex.EncodeToString(sum[:]))
}
