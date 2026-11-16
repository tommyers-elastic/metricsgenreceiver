package metricsgenreceiver

import (
	"fmt"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/distribution"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Config struct {
	StartTime                         time.Time                    `mapstructure:"start_time"`
	StartNowMinus                     time.Duration                `mapstructure:"start_now_minus"`
	EndTime                           time.Time                    `mapstructure:"end_time"`
	EndNowMinus                       time.Duration                `mapstructure:"end_now_minus"`
	Interval                          time.Duration                `mapstructure:"interval"`
	IntervalJitterStdDev              time.Duration                `mapstructure:"interval_jitter_std_dev"`
	RealTime                          bool                         `mapstructure:"real_time"`
	RunIndefinitely                   bool                         `mapstructure:"run_indefinitely"`
	ExitAfterEnd                      bool                         `mapstructure:"exit_after_end"`
	ExitAfterEndTimeout               time.Duration                `mapstructure:"exit_after_end_timeout"`
	Seed                              int64                        `mapstructure:"seed"`
	InstanceOffset                    uint                         `mapstructure:"instance_offset"`
	StaggerInstances                  bool                         `mapstructure:"stagger_instances"`
	Scenarios                         []ScenarioCfg                `mapstructure:"scenarios"`
	Distribution                      distribution.DistributionCfg `mapstructure:"distribution"`
	ExponentialHistogramsTemplatePath string                       `mapstructure:"exponential_histograms_template_path"`
}

type ScenarioCfg struct {
	Path                string         `mapstructure:"path"`
	Scale               int            `mapstructure:"scale"`
	Concurrency         int            `mapstructure:"concurrency"`
	Churn               *ChurnCfg      `mapstructure:"churn"`
	TemplateVars        map[string]any `mapstructure:"template_vars"`
	TemporalityOverride string         `mapstructure:"temporality_override"`
	HistogramOverride   string         `mapstructure:"histogram_override"`
	// Instances declares the scenario's instances explicitly instead of generating scale instances.
	Instances []InstanceCfg `mapstructure:"instances"`
	// InstancesPath points to an NDJSON file with one instance definition per line.
	InstancesPath string `mapstructure:"instances_path"`
}

// InstanceCfg is an explicitly declared instance of a scenario.
type InstanceCfg struct {
	// ResourceAttributes are upserted into every resource of the scenario's metrics template.
	ResourceAttributes map[string]any `mapstructure:"resource_attributes"`
	// TemplateVars are shallow-merged over the scenario's template_vars (instance wins).
	TemplateVars map[string]any `mapstructure:"template_vars"`
	// IntervalOffset is the instance's fixed offset within the interval. It wins over
	// stagger_instances; nil means unset.
	IntervalOffset *time.Duration `mapstructure:"interval_offset"`
	// StartTimestamp is reported as the start timestamp of the instance's data points instead of
	// the receiver's start_time; nil means unset. Defaults to StartTime when that is set.
	StartTimestamp *time.Time `mapstructure:"start_timestamp"`
	// StartTime and EndTime bound the samples the instance reports to [StartTime, EndTime);
	// nil means unbounded.
	StartTime *time.Time `mapstructure:"start_time"`
	EndTime   *time.Time `mapstructure:"end_time"`
	// Restarts are the times at which the instance restarts, in strictly ascending order.
	Restarts []time.Time `mapstructure:"restarts"`
}

type ChurnCfg struct {
	InstanceLifetime time.Duration `mapstructure:"instance_lifetime"`
	SamplesPerSeries int           `mapstructure:"samples_per_series"`
}

// churnRate is the number of instance replacements to apply per collection interval.
// It is stored as a fraction so sub-interval replacement rates can accumulate exactly.
type churnRate struct {
	replacements int64
	intervals    int64
}

func (r churnRate) enabled() bool {
	return r.replacements > 0 && r.intervals > 0
}

func newChurnRate(churn *ChurnCfg, scale int, interval time.Duration) churnRate {
	if churn == nil {
		return churnRate{}
	}
	if churn.SamplesPerSeries > 0 {
		return reduceChurnRate(int64(scale), int64(churn.SamplesPerSeries))
	}

	return reduceChurnRate(int64(scale)*int64(interval), int64(churn.InstanceLifetime))
}

func reduceChurnRate(replacements, intervals int64) churnRate {
	if replacements <= 0 || intervals <= 0 {
		return churnRate{}
	}

	// Keep the accumulator denominator small without changing the replacement rate.
	divisor := gcdInt64(replacements, intervals)
	return churnRate{
		replacements: replacements / divisor,
		intervals:    intervals / divisor,
	}
}

func gcdInt64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (c ScenarioCfg) AggregationTemporalityOverride() pmetric.AggregationTemporality {
	switch c.TemporalityOverride {
	case "cumulative":
		return pmetric.AggregationTemporalityCumulative
	case "delta":
		return pmetric.AggregationTemporalityDelta
	default:
		return pmetric.AggregationTemporalityUnspecified
	}
}

// hasExplicitInstances reports whether the scenario declares its instances via instances or instances_path.
func (c ScenarioCfg) hasExplicitInstances() bool {
	return len(c.Instances) > 0 || c.InstancesPath != ""
}

func (c Config) GetExponentialHistogramsTemplatePath() string {
	if c.ExponentialHistogramsTemplatePath != "" {
		return c.ExponentialHistogramsTemplatePath
	}
	return "builtin/exponential-histograms-low-frequency.ndjson"
}

func (c ScenarioCfg) ForceExponentialHistograms() bool {
	return c.HistogramOverride == "exponential"
}

func createDefaultConfig() component.Config {
	return &Config{
		Seed:         0,
		Scenarios:    make([]ScenarioCfg, 0),
		Distribution: distribution.DefaultDistribution,
	}
}

func (cfg *Config) Validate() error {
	if cfg.Interval.Seconds() < 1 {
		return fmt.Errorf("the interval has to be set to at least 1 second (1s)")
	}

	if cfg.StartTime.After(cfg.EndTime) {
		return fmt.Errorf("start_time must be before end_time")
	}

	if cfg.RealTime && cfg.StartNowMinus > 0 {
		return fmt.Errorf("start_now_minus cannot be used with real_time: true (results in permanent timestamp lag)")
	}

	if cfg.RunIndefinitely && (!cfg.EndTime.IsZero() || cfg.EndNowMinus > 0) {
		return fmt.Errorf("run_indefinitely cannot be combined with end_time or end_now_minus")
	}

	for _, scn := range cfg.Scenarios {
		if scn.hasExplicitInstances() {
			if err := scn.validateExplicitInstances(cfg); err != nil {
				return err
			}
			continue
		}
		if scn.Concurrency != 0 && scn.Scale%scn.Concurrency != 0 {
			return fmt.Errorf("scale must be a multiple of concurrency")
		}
		if scn.Concurrency < 0 {
			return fmt.Errorf("concurrency must be a positive number")
		}
		if scn.Churn == nil {
			continue
		}
		if scn.Scale <= 0 {
			return fmt.Errorf("scale must be positive when churn is enabled")
		}
		hasInstanceLifetime := scn.Churn.InstanceLifetime != 0
		hasSamplesPerSeries := scn.Churn.SamplesPerSeries != 0
		if hasInstanceLifetime == hasSamplesPerSeries {
			return fmt.Errorf("exactly one of churn.samples_per_series or churn.instance_lifetime must be set")
		}
		if hasSamplesPerSeries && scn.Churn.SamplesPerSeries < 1 {
			return fmt.Errorf("churn.samples_per_series must be greater than or equal to 1")
		}
		if !hasInstanceLifetime {
			continue
		}
		if scn.Churn.InstanceLifetime < 0 {
			return fmt.Errorf("churn.instance_lifetime must be a positive duration")
		}
		if scn.Churn.InstanceLifetime < cfg.Interval {
			return fmt.Errorf("churn.instance_lifetime must be greater than or equal to interval")
		}
	}
	return nil
}

func (c ScenarioCfg) validateExplicitInstances(cfg *Config) error {
	if len(c.Instances) > 0 && c.InstancesPath != "" {
		return fmt.Errorf("instances and instances_path are mutually exclusive")
	}
	if c.Concurrency != 0 {
		return fmt.Errorf("concurrency is not supported with instances or instances_path")
	}
	if c.Churn != nil {
		return fmt.Errorf("churn is not supported with instances or instances_path")
	}
	if c.Scale < 0 {
		return fmt.Errorf("scale must not be negative")
	}
	if c.InstancesPath != "" {
		// The file is loaded and checked when the receiver is created.
		return nil
	}
	if err := checkInstanceScale(c.Scale, len(c.Instances)); err != nil {
		return err
	}
	for i, inst := range c.Instances {
		if err := inst.validate(cfg.Interval); err != nil {
			return fmt.Errorf("%s: %w", inlineInstanceSource(i), err)
		}
		// When start_time is derived from start_now_minus, this is checked when the receiver is created.
		if !cfg.StartTime.IsZero() {
			if err := inst.validateStart(cfg.StartTime); err != nil {
				return fmt.Errorf("%s: %w", inlineInstanceSource(i), err)
			}
		}
	}
	return nil
}

func checkInstanceScale(scale, instances int) error {
	if scale > instances {
		return fmt.Errorf("scale (%d) must not exceed the number of instances (%d)", scale, instances)
	}
	return nil
}
