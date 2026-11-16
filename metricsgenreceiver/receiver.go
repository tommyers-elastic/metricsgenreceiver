package metricsgenreceiver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/distribution"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/expohistogen"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/metadata"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/metricstmpl"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type MetricsGenReceiver struct {
	cfg       *Config
	obsreport *receiverhelper.ObsReport
	settings  receiver.Settings

	baseRand    *rand.Rand // base random number generator seeded with the configured seed
	expHistoGen *expohistogen.Generator
	nextMetrics consumer.Metrics
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	scenarios   []Scenario
	progress    *MetricsProgress
}

type Scenario struct {
	config                     ScenarioCfg
	metricsTemplate            *pmetric.Metrics
	resourceAttributesTemplate pcommon.Resource
	instances                  []scenarioInstance
	precision                  map[string]int
	generationHints            map[string]distribution.MetricGenerationHint
	nextReplacementInstanceID  int
	churnRate                  churnRate
	churnRemainder             int64
	// seriesIdentityHashes holds IdentityHash(metric name, attrs) for each template data point in
	// the order ForEachDataPoint visits them. Precomputed once so the per-instance emission path
	// avoids re-hashing the attribute set on every tick.
	seriesIdentityHashes []uint64
	// seriesPlans holds the resolved per-series variation strategy and precision for each number
	// data point, indexed like seriesIdentityHashes. Precomputed once so the per-instance emission
	// path avoids per-data-point hint/precision map lookups and strategy dispatch. Entries for
	// non-number data points carry a zero-value plan and are unused.
	seriesPlans []distribution.NumberSeriesPlan
	// explicitInstances is set when the instances were declared via instances or instances_path.
	explicitInstances bool
	// configIndex is the position of the scenario's configuration in Config.Scenarios.
	configIndex int
	// resetSlots maps each template data point to its slot in an instance's counter baseline, or
	// -1 if the series is not reset on (re)start; see instanceLifecycle. Only set for explicit
	// instances with a lifecycle.
	resetSlots      []int
	resetSlotCounts baselineSize
}

type scenarioInstance struct {
	id       int
	resource pcommon.Resource
	// explicitAttributes holds the resource attributes declared on an explicit instance. They are
	// upserted into every resource of the emitted metrics. Only set when the scenario has
	// explicitInstances.
	explicitAttributes pcommon.Map
	// intervalOffset is added to the tick time to get the instance's data point timestamps.
	intervalOffset time.Duration
	// startTimestamp, when not zero, replaces the start timestamp of the instance's data points.
	// With a lifecycle, it is the initial start timestamp before any restart.
	startTimestamp time.Time
	// lifecycle is set for explicit instances with start_time, end_time or restarts.
	lifecycle *instanceLifecycle
}

type MetricsProgress struct {
	start      time.Time
	datapoints atomic.Uint64
}

func newMetricsProgress() *MetricsProgress {
	return &MetricsProgress{
		start:      time.Now(),
		datapoints: atomic.Uint64{},
	}
}

func (p *MetricsProgress) duration() time.Duration {
	return time.Since(p.start)
}
func (p *MetricsProgress) dataPointsPerSecond() float64 {
	return float64(p.datapoints.Load()) / p.duration().Seconds()
}

func (p *MetricsProgress) eta(progressPct float64) time.Duration {
	if progressPct == 0 {
		return time.Duration(0)
	}
	return time.Duration(float64(p.duration().Nanoseconds())/progressPct) - p.duration()
}

func newMetricsGenReceiver(cfg *Config, set receiver.Settings) (*MetricsGenReceiver, error) {
	obsreport, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	now := time.Now()
	if cfg.StartTime.IsZero() {
		cfg.StartTime = now.Add(-cfg.StartNowMinus)
	}
	if cfg.EndTime.IsZero() {
		if cfg.RunIndefinitely {
			cfg.EndTime = time.Unix(math.MaxInt64/int64(time.Second), 0)
		} else {
			cfg.EndTime = now.Add(-cfg.EndNowMinus)
		}
	}

	baseRand := rand.New(rand.NewSource(cfg.Seed))
	expHistoGen, err := expohistogen.NewGenerator(cfg.GetExponentialHistogramsTemplatePath())
	if err != nil {
		return nil, err
	}

	scenarios := make([]Scenario, 0, len(cfg.Scenarios))
	for scnIdx, scn := range cfg.Scenarios {
		if scn.hasExplicitInstances() {
			explicitScenarios, err := newExplicitInstanceScenarios(cfg, scnIdx, scn, baseRand, expHistoGen)
			if err != nil {
				return nil, err
			}
			scenarios = append(scenarios, explicitScenarios...)
			continue
		}

		metrics, generationHints, err := prepareMetricsTemplate(cfg, scn, scn.TemplateVars, baseRand, expHistoGen)
		if err != nil {
			return nil, err
		}
		resources, err := metricstmpl.GetResources(scn.Path, cfg.StartTime, scn.Scale, scn.TemplateVars, baseRand, cfg.InstanceOffset)
		if err != nil {
			return nil, err
		}
		instances := make([]scenarioInstance, len(resources))
		for i, resource := range resources {
			id := i + int(cfg.InstanceOffset)
			instances[i] = scenarioInstance{
				id:             id,
				resource:       resource,
				intervalOffset: staggerOffset(cfg, scnIdx, id),
			}
		}
		scenario := newScenario(scn, &metrics, generationHints, instances)
		scenario.configIndex = scnIdx
		scenario.nextReplacementInstanceID = scn.Scale
		scenario.churnRate = newChurnRate(scn.Churn, scn.Scale, cfg.Interval)
		scenarios = append(scenarios, scenario)
	}

	return &MetricsGenReceiver{
		cfg:         cfg,
		settings:    set,
		baseRand:    baseRand,
		expHistoGen: expHistoGen,
		obsreport:   obsreport,
		scenarios:   scenarios,
		progress:    newMetricsProgress(),
	}, nil
}

// prepareMetricsTemplate renders the scenario's metrics template with the given variables and
// prepares it for generation. Exponential histogram data points draw from baseRand.
func prepareMetricsTemplate(cfg *Config, scn ScenarioCfg, templateVars map[string]any, baseRand *rand.Rand, expHistoGen *expohistogen.Generator) (pmetric.Metrics, map[string]distribution.MetricGenerationHint, error) {
	metrics, err := metricstmpl.RenderMetricsTemplate(scn.Path, templateVars)
	if err != nil {
		return pmetric.Metrics{}, nil, err
	}
	generationHints, err := metricstmpl.ExtractGenerationHints(&metrics)
	if err != nil {
		return pmetric.Metrics{}, nil, err
	}

	if scn.ForceExponentialHistograms() {
		dp.ForEachMetric(&metrics, func(res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric) {
			replaceHistogramsWithExponentialHistograms(m)
		})
	}
	dp.ForEachDataPoint(&metrics, func(_ int, res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric, dp dp.DataPoint) {
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(cfg.StartTime))
		if scn.AggregationTemporalityOverride() != pmetric.AggregationTemporalityUnspecified {
			switch m.Type() {
			case pmetric.MetricTypeSum:
				m.Sum().SetAggregationTemporality(scn.AggregationTemporalityOverride())
			case pmetric.MetricTypeHistogram:
				m.Histogram().SetAggregationTemporality(scn.AggregationTemporalityOverride())
			case pmetric.MetricTypeExponentialHistogram:
				m.ExponentialHistogram().SetAggregationTemporality(scn.AggregationTemporalityOverride())
			default:
				// no-op
			}
		}
		if m.Type() == pmetric.MetricTypeExponentialHistogram {
			expHistoGen.GenerateInto(baseRand, dp.(pmetric.ExponentialHistogramDataPoint))
			if scn.AggregationTemporalityOverride() == pmetric.AggregationTemporalityCumulative {
				m.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			} else {
				m.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			}
		}
	})
	return metrics, generationHints, nil
}

// newScenario builds a Scenario from a prepared metrics template and its instances,
// precomputing the per-series data used on the emission path.
func newScenario(scn ScenarioCfg, metrics *pmetric.Metrics, generationHints map[string]distribution.MetricGenerationHint, instances []scenarioInstance) Scenario {
	precision := distribution.InferPrecision(metrics)
	seriesIdentityHashes := make([]uint64, metrics.DataPointCount())
	seriesPlans := make([]distribution.NumberSeriesPlan, metrics.DataPointCount())
	dp.ForEachDataPoint(metrics, func(idx int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		seriesIdentityHashes[idx] = distribution.IdentityHash(m.Name(), dataPoint.Attributes())
		if _, isNumber := dataPoint.(pmetric.NumberDataPoint); isNumber {
			seriesPlans[idx] = distribution.PlanNumberSeries(m, generationHints, precision)
		}
	})
	return Scenario{
		config:               scn,
		metricsTemplate:      metrics,
		instances:            instances,
		precision:            precision,
		generationHints:      generationHints,
		seriesIdentityHashes: seriesIdentityHashes,
		seriesPlans:          seriesPlans,
	}
}

// newExplicitInstanceScenarios builds one Scenario per group of explicit instances with equal
// merged template variables, so instances with the same shape share one metrics template.
// Groups are ordered by first appearance; within a group, instances keep list order.
func newExplicitInstanceScenarios(cfg *Config, scnIdx int, scn ScenarioCfg, baseRand *rand.Rand, expHistoGen *expohistogen.Generator) ([]Scenario, error) {
	declared, err := scenarioInstances(scn, cfg.Interval)
	if err != nil {
		return nil, err
	}
	for _, inst := range declared {
		if err := inst.cfg.validateStart(cfg.StartTime); err != nil {
			return nil, fmt.Errorf("%s: %w", inst.source, err)
		}
	}
	groups, err := groupInstancesByTemplateVars(scn.TemplateVars, declared)
	if err != nil {
		return nil, err
	}
	hasResourceTemplate, err := metricstmpl.ResourceAttributesTemplateExists(scn.Path)
	if err != nil {
		return nil, err
	}
	startTimeString := cfg.StartTime.Format(time.RFC3339)

	scenarios := make([]Scenario, 0, len(groups))
	for _, group := range groups {
		metrics, generationHints, err := prepareMetricsTemplate(cfg, scn, group.templateVars, baseRand, expHistoGen)
		if err != nil {
			return nil, err
		}
		instances := make([]scenarioInstance, 0, len(group.indexes))
		hasLifecycle := false
		for _, i := range group.indexes {
			resource := pcommon.NewResource()
			if hasResourceTemplate {
				resource, err = metricstmpl.RenderResource(scn.Path, i, startTimeString, group.templateVars, baseRand, cfg.InstanceOffset)
				if err != nil {
					return nil, err
				}
			}
			explicitAttributes, err := declared[i].cfg.resourceAttributes()
			if err != nil {
				return nil, fmt.Errorf("%s: %w", declared[i].source, err)
			}
			id := i + int(cfg.InstanceOffset)
			instance := scenarioInstance{
				id:                 id,
				resource:           resource,
				explicitAttributes: explicitAttributes,
				intervalOffset:     declared[i].cfg.intervalOffset(staggerOffset(cfg, scnIdx, id)),
			}
			if startTimestamp := declared[i].cfg.initialStartTimestamp(); startTimestamp != nil {
				instance.startTimestamp = *startTimestamp
			}
			if declared[i].cfg.hasLifecycle() {
				instance.lifecycle = newInstanceLifecycle(declared[i].cfg)
				hasLifecycle = true
			}
			instances = append(instances, instance)
		}
		scenario := newScenario(scn, &metrics, generationHints, instances)
		scenario.explicitInstances = true
		scenario.configIndex = scnIdx
		if hasLifecycle {
			scenario.resetSlots, scenario.resetSlotCounts = resetSlotsFor(&metrics, generationHints)
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

func (r *MetricsGenReceiver) Start(ctx context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		nextLog := r.progress.start.Add(10 * time.Second)
		ticker := time.NewTicker(r.cfg.Interval)
		defer ticker.Stop()
		currentTime := r.cfg.StartTime
		for i := 0; currentTime.Before(r.cfg.EndTime); i++ {
			if ctx.Err() != nil {
				return
			}
			if time.Now().After(nextLog) {
				progressPct := currentTime.Sub(r.cfg.StartTime).Seconds() / r.cfg.EndTime.Sub(r.cfg.StartTime).Seconds()
				r.settings.Logger.Info("generating metrics progress",
					zap.Int("progress_percent", int(progressPct*100)),
					zap.String("eta", r.progress.eta(progressPct).Round(time.Second).String()),
					zap.Uint64("datapoints", r.progress.datapoints.Load()),
					zap.Float64("data_points_per_second", r.progress.dataPointsPerSecond()),
				)
				nextLog = nextLog.Add(10 * time.Second)
			}
			r.progress.datapoints.Add(r.produceMetrics(ctx, currentTime))
			r.applyChurn(currentTime)

			if r.cfg.RealTime {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return
				}
			}
			currentTime = currentTime.Add(r.cfg.Interval)
		}
		if r.cfg.ExitAfterEnd {
			// After the runner has finished generating metrics, we wait for the configured duration before exiting.
			if r.cfg.ExitAfterEndTimeout > 0 {
				r.settings.Logger.Info("finished generating metrics, waiting before exiting",
					zap.Duration("exit_after_end_timeout", r.cfg.ExitAfterEndTimeout),
				)
				time.Sleep(r.cfg.ExitAfterEndTimeout)
			} else {
				r.settings.Logger.Info("finished generating metrics, exiting immediately")
			}

			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errors.New("exiting because exit_after_end is set to true")))
		}
	}()

	return nil
}

func replaceHistogramsWithExponentialHistograms(m pmetric.Metric) {
	if m.Type() == pmetric.MetricTypeHistogram {
		// Get the histogram data
		histogram := m.Histogram()
		histogramDPs := histogram.DataPoints()

		// Store only the data point information declared in the DataPoint interface
		type dpInfo struct {
			startTimestamp pcommon.Timestamp
			timestamp      pcommon.Timestamp
			attributes     pcommon.Map
		}

		dpInfos := make([]dpInfo, histogramDPs.Len())
		for i := 0; i < histogramDPs.Len(); i++ {
			dp := histogramDPs.At(i)
			dpInfos[i] = dpInfo{
				startTimestamp: dp.StartTimestamp(),
				timestamp:      dp.Timestamp(),
				attributes:     dp.Attributes(),
			}
		}

		// Convert to exponential histogram
		expHist := m.SetEmptyExponentialHistogram()
		expHist.SetAggregationTemporality(histogram.AggregationTemporality())

		// Create exponential histogram data points preserving the series
		expDPs := expHist.DataPoints()
		expDPs.EnsureCapacity(len(dpInfos))

		for _, info := range dpInfos {
			expDP := expDPs.AppendEmpty()
			expDP.SetStartTimestamp(info.startTimestamp)
			expDP.SetTimestamp(info.timestamp)
			info.attributes.CopyTo(expDP.Attributes())
		}
	}

}

func addJitter(t time.Time, stdDev time.Duration, interval time.Duration, ra *rand.Rand) time.Time {
	if stdDev == 0 {
		return t
	}
	jitter := time.Duration(int64(math.Abs(ra.NormFloat64() * float64(stdDev))))
	if jitter >= interval {
		jitter = interval - 1
	}
	return t.Add(jitter)
}

// staggerOffset returns the fixed offset within the interval at which an instance reports when
// stagger_instances is enabled, and 0 otherwise. It is a pure function of the seed, the scenario's
// position in the configuration and the instance ID, and uses integer arithmetic only, so it is
// identical on every architecture and does not draw from the shared random number generator.
func staggerOffset(cfg *Config, scenarioIndex int, instanceID int) time.Duration {
	if !cfg.StaggerInstances || cfg.Interval <= 0 {
		return 0
	}
	h := mix64(uint64(cfg.Seed))
	h = mix64(h ^ uint64(scenarioIndex))
	h = mix64(h ^ uint64(instanceID))
	return time.Duration(h % uint64(cfg.Interval))
}

// mix64 is the splitmix64 finalizer.
func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (r *MetricsGenReceiver) applyChurn(simulatedTime time.Time) {
	for idx := range r.scenarios {
		scn := &r.scenarios[idx]
		churn := r.consumeNextChurnCount(scn)
		if churn == 0 || len(scn.instances) == 0 {
			continue
		}

		startTime := simulatedTime.Format(time.RFC3339)
		for i := 0; i < churn; i++ {
			id := scn.nextReplacementInstanceID
			scn.nextReplacementInstanceID++
			resource, err := metricstmpl.RenderResource(scn.config.Path, id, startTime, scn.config.TemplateVars, r.baseRand, r.cfg.InstanceOffset)
			if err != nil {
				r.settings.Logger.Error("failed to apply churn", zap.Error(err))
			} else {
				instanceID := id + int(r.cfg.InstanceOffset)
				scn.instances[id%len(scn.instances)] = scenarioInstance{
					id:             instanceID,
					resource:       resource,
					intervalOffset: staggerOffset(r.cfg, scn.configIndex, instanceID),
				}
			}
		}
	}
}

func (r *MetricsGenReceiver) consumeNextChurnCount(scn *Scenario) int {
	if !scn.churnRate.enabled() {
		return 0
	}

	// Carry the fractional remainder forward so slower churn rates are spread across intervals.
	scn.churnRemainder += scn.churnRate.replacements
	churn := scn.churnRemainder / scn.churnRate.intervals
	scn.churnRemainder %= scn.churnRate.intervals
	return int(churn)
}

func (r *MetricsGenReceiver) produceMetrics(ctx context.Context, currentTime time.Time) uint64 {
	dataPoints := new(uint64)
	wg := sync.WaitGroup{}
	for idx := range r.scenarios {
		scn := &r.scenarios[idx]

		// we don't keep track of the data points for each instance individually to reduce memory pressure
		// we still advance the metrics template have a new baseline that's used when simulating the metrics for each individual instance
		// this makes sure counters are increasing over time
		dp.ForEachDataPoint(scn.metricsTemplate, func(_ int, res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
			opts := distribution.AdvanceOptions{
				Interval:  r.cfg.Interval,
				Precision: scn.precision,
			}
			if hint, ok := scn.generationHints[m.Name()]; ok {
				opts.Hint = &hint
			}
			distribution.AdvanceDataPoint(dataPoint, r.baseRand, m, r.cfg.Distribution, r.expHistoGen, opts)
		})
		if scn.config.Concurrency == 0 {
			for i := range scn.instances {
				instance := &scn.instances[i]
				if lifecycle := instance.lifecycle; lifecycle != nil {
					sampleTime := currentTime.Add(instance.intervalOffset)
					if lifecycle.ended(sampleTime) {
						lifecycle.dropBaseline()
					}
					// An instance outside its lifetime emits nothing and draws nothing from the RNG.
					if !lifecycle.active(sampleTime) {
						continue
					}
				}
				*dataPoints += uint64(r.produceMetricsForInstance(ctx, r.baseRand, currentTime, scn, instance))
			}
			continue
		}

		// Explicit instances, and so lifetimes, never get here: concurrency is rejected for them.
		for i := 0; i < scn.config.Concurrency; i++ {
			// Use a new random number generator for each goroutine to avoid race conditions
			rng := r.getNewRand()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < scn.config.Scale/scn.config.Concurrency; j++ {
					instance := &scn.instances[j+i*scn.config.Scale/scn.config.Concurrency]
					currentDataPoints := r.produceMetricsForInstance(ctx, rng, currentTime, scn, instance)
					atomic.AddUint64(dataPoints, uint64(currentDataPoints))
				}
			}()
		}
	}
	wg.Wait()
	return *dataPoints
}

func (r *MetricsGenReceiver) produceMetricsForInstance(ctx context.Context, rng *rand.Rand, currentTime time.Time, scn *Scenario, instance *scenarioInstance) int {
	r.obsreport.StartMetricsOp(ctx)
	metrics := pmetric.NewMetrics()
	scn.metricsTemplate.CopyTo(metrics)
	resourceMetrics := metrics.ResourceMetrics()
	for j := 0; j < resourceMetrics.Len(); j++ {
		overrideExistingAttributes(instance.resource, resourceMetrics.At(j).Resource())
		if scn.explicitInstances {
			upsertAttributes(instance.explicitAttributes, resourceMetrics.At(j).Resource())
		}
	}

	// The tick time drives the template and the time-based variation; the instance reports at its
	// interval offset after the tick, plus jitter.
	sampleTime := currentTime.Add(instance.intervalOffset)
	instanceTime := addJitter(sampleTime, r.cfg.IntervalJitterStdDev, r.cfg.Interval, rng)
	effectiveStart := instance.startTimestamp
	var baseline *counterBaseline
	var baselineTick time.Time
	if lifecycle := instance.lifecycle; lifecycle != nil {
		effectiveStart = lifecycle.effectiveStart(instance.startTimestamp, sampleTime)
		// An instance that (re)started inside the window counts its counter series from zero:
		// subtract the template values at the first tick of this start, and restart their
		// time-dependent counter variation at that tick.
		if effectiveStart.After(r.cfg.StartTime) {
			if !lifecycle.baselineStart.Equal(effectiveStart) {
				lifecycle.snapshotBaseline(scn, effectiveStart, currentTime)
			}
			baseline = lifecycle.baseline
			baselineTick = lifecycle.baselineTick
		}
	}
	hasStartTimestamp := !effectiveStart.IsZero()
	startTimestamp := pcommon.NewTimestampFromTime(effectiveStart)

	dp.ForEachDataPoint(&metrics, func(idx int, res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		variationStart := r.cfg.StartTime
		if baseline != nil {
			if slot := scn.resetSlots[idx]; slot >= 0 {
				baseline.subtract(dataPoint.(pmetric.NumberDataPoint), slot)
				variationStart = baselineTick
			}
		}
		distribution.EmitForInstance(dataPoint, m, distribution.InstanceEmitOptions{
			InstanceID:   instance.id,
			IdentityHash: scn.seriesIdentityHashes[idx],
			Plan:         scn.seriesPlans[idx],
			Hints:        scn.generationHints,
			Precision:    scn.precision,
			Variation: distribution.InstanceVariationOptions{
				Timestamp:        currentTime,
				StartTime:        variationStart,
				Interval:         r.cfg.Interval,
				CounterBaseDelta: float64(r.cfg.Distribution.MedianMonotonicSum),
			},
			Rand:        rng,
			Dist:        r.cfg.Distribution,
			ExpHistoGen: r.expHistoGen,
		})
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(instanceTime))
		if hasStartTimestamp {
			dataPoint.SetStartTimestamp(startTimestamp)
		}
	})
	dataPoints := metrics.DataPointCount()
	err := r.nextMetrics.ConsumeMetrics(ctx, metrics)
	r.obsreport.EndMetricsOp(ctx, metadata.Type.String(), dataPoints, err)
	return dataPoints
}

func overrideExistingAttributes(source, target pcommon.Resource) {
	targetAttr := target.Attributes()
	source.Attributes().Range(func(k string, v pcommon.Value) bool {
		if _, exists := targetAttr.Get(k); exists {
			targetValue := targetAttr.PutEmpty(k)
			v.CopyTo(targetValue)
		}
		return true
	})
}

// upsertAttributes copies every attribute of source into target, overwriting existing keys.
func upsertAttributes(source pcommon.Map, target pcommon.Resource) {
	targetAttr := target.Attributes()
	source.Range(func(k string, v pcommon.Value) bool {
		v.CopyTo(targetAttr.PutEmpty(k))
		return true
	})
}

func (r *MetricsGenReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	r.settings.Logger.Info("finished generating metrics",
		zap.Uint64("datapoints", r.progress.datapoints.Load()),
		zap.String("duration", r.progress.duration().Round(time.Millisecond).String()),
		zap.Float64("data_points_per_second", r.progress.dataPointsPerSecond()),
	)
	return nil
}

// getNewRand returns a new random number generator seeded with the configured seed.
// This is NOT thread-safe, so it should only be used in a single goroutine.
func (r *MetricsGenReceiver) getNewRand() *rand.Rand {
	return rand.New(rand.NewSource(r.baseRand.Int63()))
}
