# Metrics generation receiver

| Status        |                          |
| ------------- |--------------------------|
| Stability     | development: metrics     |

Generates metrics given an initial OTLP JSON file that was produced with the `fileexporter`.
This receiver is inspired by the metrics dataset generation tool that's part of https://github.com/timescale/tsbs.
The difference is that this makes it easier to use real-ish OTel metrics from a receiver such as the `hostmetricsreceiver`
and send it to different backends using the corresponding exporter.

Given an initial set of resource metrics, this receiver generates metrics with a configurable scale, start time, end time, and interval.
For example, given the output of a single report from the `hostmetricsreceiver`,
lets you generate a day's worth of data from multiple simulated hosts with a given interval.

Generation runs in two stages per interval.
First, the shared metrics template is advanced stochastically using the `distribution` settings — this produces the baseline that every simulated instance sees.
Second, each instance emits a copy of the template with a deterministic per-series variation layered on top, so instances differ from each other while each instance's series stays coherent over time.
The variation for number data points is controlled by optional generation hints; histograms are still randomized per instance.

## Getting Started

### Quick Start

To get started quickly, [check out the releases page](https://github.com/elastic/metricsgenreceiver/releases) for compiled binaries you can use to get started right away.

### Building

metricsgenreceiver is a receiver for the otel collector. To build the otelcollector, the tool
[ocb](https://opentelemetry.io/docs/collector/custom-collector/) is needed. To install it on OS X, run

```bash
curl --proto '=https' --tlsv1.2 -fL -o ocb \
https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/cmd%2Fbuilder%2Fv0.159.0/ocb_0.159.0_darwin_arm64
chmod +x ocb
```

Be aware, currently this exact version is needed (v0.159.0).

Then run the following command:

```bash
./ocb --config builder-config.yaml
```

This will build the otel collector binary under `./otelcol-dev/otelcol`.

You can start the Otel Collector with the predefined config:

```
./otelcol-dev/otelcol --config otelcol.yaml
```

For local development, you can use the `otelcol.dev.yaml` config file which is ignored by default.

There is also a dedicated `node_exporter` example config in `otelcol-node_exporter.yaml`.


### Receiver Settings

It is possible to adjust the metricsgen receiver with the configs listed below to enable different scenarios. To do this, adjust the section below:

```
receivers:
  metricsgen:
    ...
```

* `start_time`: the start time of the generated metrics (inclusive).
* `start_now_minus`: the duration to subtract from the current time to set the start time.
  Note that when using this option, the data generation will not be deterministic.
* `end_time`: the end time of the generated metrics (exclusive).
* `end_now_minus`: the duration to subtract from the current time to set the end time.
  Note that when using this option, the data generation will not be deterministic.
* `interval`: the interval at which the metrics are simulated.
  The minimum value is 1s.
* `interval_jitter_std_dev` (default `0`): when set to a non-zero value (such as `5ms`), a jitter is added to the interval.
  The jitter is equal to the absolute value of a normal distribution with a median of 0ms and a standard deviation of the specified value.
  It is capped by the interval, so that the next interval will never start at or before the previous one.
  This simulates real-world scenarios where the collection interval is sometimes a bit late due to delays running the scheduled metric collection.
  When set to a non-zero value, this can impact the effectiveness of the compression that a metric datastore may apply.
* `real_time` (default `false`): when set to true, the receiver will pace metric emission
  to the configured `interval` instead of generating as fast as possible.
* `run_indefinitely` (default `false`): when set to true, the receiver will generate metrics
  indefinitely until stopped. Cannot be combined with `end_time` or `end_now_minus`.
* `exit_after_end` (default `false`): when set to true, will terminate the collector.
* `exit_after_end_timeout` (default `0`): timeout in case exit_after_end is set to true before the collector is terminated.
* `seed` (default `0`): seed value for the random number generator that's used for simulating the standard distribution. The seed makes sure that the data generation is deterministic.
  Generated instances share one random number generator seeded with it; explicitly declared instances (see `instances`) derive their own random streams from it and from their definition instead.
* `instance_offset` (default `0`): when set, the instance ID of each metric will be offset by the specified value.
* `stagger_instances` (default `false`): when set to true, every instance reports at its own fixed offset within the `interval`, instead of all instances reporting at the start of each interval.
  This simulates collectors that do not all report at the same instant.
  The offset is in `[0, interval)` and is derived from the `seed`, the position of the scenario in `scenarios`, and the instance ID, so it is stable for an instance (also for instances created by `churn`) and does not change any other generated value.
  For an explicitly declared instance, the offset is instead derived from the instance's key (see `instances`), so it does not depend on the position of the instance or of its scenario.
  The data point timestamps of an instance at the tick `t` are `t + offset`, plus the jitter if `interval_jitter_std_dev` is set; they lie in `[t, t + interval)` before jitter.
  Metric values are the same as without staggering; only timestamps change.
  Every instance still reports one sample per interval, so all instances report the same number of samples for any time window.
  As a sample's timestamps lie up to one `interval` after its tick, they stay before `end_time` (before jitter) only when the time window is a multiple of `interval`.
  With `real_time: true`, staggered samples are sent at the tick, so their timestamps can be up to one `interval` ahead of the time they are sent.
* `exponential_histograms_template_path` (default `builtin/exponential-histograms-low-frequency.ndjson`): path to the template file used for generating exponential histogram data points.
  The file should be in NDJSON format containing OTLP-JSON data as generated by the OpenTelemetry Collector `fileexporter`.
  The receiver will extract all exponential histogram samples with delta temporality from the template and use those as a basis for generating exponential histograms.
  The histograms will be further randomized by slightly adjusting the buckets and their counts, but the overall distribution will remain the same.
  Built-in templates:
  * `builtin/exponential-histograms-low-frequency.ndjson`: exponential histogram data with a low number of observations per histogram.
  * `builtin/exponential-histograms-high-frequency.ndjson`: exponential histogram data with a high number of observations per histogram.
* `distribution`: controls how the shared metrics template is advanced each interval,
  taking into account the [temporality](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#temporality),
  [monotonicity](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#point-kinds),
  and capping floating point gauges values whose initial value is between 0 and 1 to that range.
  By changing the distribution parameters, you can simulate how predictable the metrics are evolving over time.
  This may have an impact on how well the backend can compress the data.
  These settings only affect the shared template; per-instance variation for number data points is controlled separately by generation hints.
  * `median_monotonic_sum` (default `100`): the median value for the normal distribution used to simulate metrics that are monotonic sums.
    If the sum is not monotonic, or if the metric is a gauge, the median will always be zero, to simulate a value that goes up and down over time.
    If the temporality of the metric is delta, the new value will be set to the result of the normal distribution.
    Otherwise (if the metric is using cumulative or unspecified temporality), the new value will be incremented by the result of the normal distribution.
  * `std_dev_gauge_pct` (default `0.05`): the standard deviation used for floating point gauge metrics whose initial value is between 0 and 1.
  * `std_dev` (default `5`): the standard deviation of the normal distribution used to simulate all other metrics.
    The generated metrics will be kept within that range and the median is always 0, to simulate a value that goes up and down over time.
    The new value will be incremented by the result of the normal distribution.
* `scenarios`: a list of scenarios to simulate. For every interval, each scenario is simulated before moving to the next interval.
  * `scale`: determines how many instances (like hosts) to simulate.
    The individual instances will have a consistent set of resource attributes throughout the simulation.
    Each instance's number data points also get a deterministic per-series variation (hash-derived from instance ID + metric name + attributes) so that instances differ from each other without introducing noise into any single instance's time series.
  * `concurrency` (default `0`): when set to a non-zero value, the receiver will simulate the scenario concurrently with the specified number of goroutines.
  * `path`: the path of the scenario files.
    * Built-in scenarios:
      This receiver comes with a number of pre-packaged scenarios:
      * `builtin/hostmetrics`: simulates metrics from the `hostmetricsreceiver`.
      * `builtin/kubeletstats-node`: simulates node metrics from the `kubeletstatsreceiver`.
      * `builtin/kubeletstats-pod`: simulates pod metrics from the `kubeletstatsreceiver`.
        Each simulated pod reports metrics for the pod itself, one container, and one volume mount.
      * `builtin/tsbs-devops`: an adaptation of the [Timescale TSBS](https://github.com/timescale/tsbs) devops scenario.
      * `builtin/elasticapm-service-metrics`: simulates aggregated service metrics from the `elasticapmconnector`. The scale influences how many services are simulated.
      * `builtin/elasticapm-span-destination-metrics`: simulates aggregated span destination metrics from the `elasticapmconnector`. The scale influences how many services are simulated.Uses the `template_vars` option to customize the data generation:
        * `destinations`: the number of distinct exit span names to simulate per service. This affects the number of simulated metrics.
      * `builtin/elasticapm-transaction-metrics`: simulates aggregated transaction metrics from the `elasticapmconnector`. The scale influences how many service instances are simulated. Uses the `template_vars` option to customize the data generation:
        * `services`: the number of services to simulate. Does not affect the number of metrics.
        * `transactions`: the number of transactions to simulate per service instance. This affects the number of simulated metrics.
      * `builtin/nginx`: built-in scenario for nginx connection metrics simulating the `nginxreceiver`.
      * `builtin/node_exporter`: simulates a typical small Linux VM scraped by Prometheus `node_exporter` while keeping Prometheus metric names and target identity keys like `job` and `instance`.
    * Custom scenarios:
      Expects a `<path>.json` or `<path>.yaml` and a `<path>-resource-attributes.json` or `<path>-resource-attributes.yaml` file.
      The `<path>.json`/`<path>.yaml` file contains a single batch of resource metrics in JSON or YAML format, as produced by the `fileexporter`.
      The `<path>-resource-attributes.json`/`<path>-resource-attributes.yaml` file contains the resource attributes template.
      Optional generation hints can be declared inline on a number metric (`Gauge` or `Sum`) by adding a `metricsgen.hint.class` entry to its `metadata` block. Supported classes are `stable_binary`, `current_count`, `slow_gauge`, `steady_counter`, `sparse_counter`, `seeded_rate_counter`, `bounded_gauge`, `share_gauge`, `share_counter`, `clock`, and `constant`.
      Hints control how values evolve on the shared template and how each instance's copy varies around it deterministically. The receiver strips the hint metadata at startup so it does not leak into emitted data. Hints are enforced per metric name within a scenario: if the same metric name appears multiple times, every occurrence must declare the same hint class or none of them may declare a hint.
      Some classes take parameters, declared as further `metadata` entries with string values under `metricsgen.hint.<parameter>` (such as `metricsgen.hint.variation`). The classes that take parameters are `bounded_gauge`, `share_gauge` and `share_counter`. For these classes, every `metadata` key under `metricsgen.hint.` is the class or a parameter: parameters are stripped like the class and must be the same on every occurrence of a metric name, and an unknown parameter, a value that is not a string, or a value that cannot be parsed or is out of range is an error at startup.
      For every other class, and for a metric without `metricsgen.hint.class`, only `metricsgen.hint.class` is read and stripped: other `metadata` entries under `metricsgen.hint.` are left as they are and passed through to the emitted metadata.
      Without a hint, and with `steady_counter`, every counter of a scenario grows by about the same amount per interval (`median_monotonic_sum`).
      With `seeded_rate_counter`, each series instead keeps growing at the average rate of its seed data point: its value divided by the time between its `startTimeUnixNano` and its `timeUnixNano`, which a capture of a cumulative counter already contains.
      For example, a series that counted `86400` in the day since its start grows by about `10` per `10s` interval, while a series of the same metric that counted `864` grows by about `0.1`.
      The metric must be a `Sum`, and every data point of it must have both timestamps, with `timeUnixNano` after `startTimeUnixNano`, and a finite value that is not negative; a value of `0` keeps the series at `0`.
      Each interval adds the rate times the interval with a random variation of about 10%, rounded at random to the precision the value is kept at (whole numbers for `asInt`, the most decimal places among the metric's seed values for `asDouble`) so that slow series still grow at their rate on average; over a short window a very slow series may not move at all.
      Each instance's copy is the template value multiplied by a fixed factor between `0.5` and `1.5` that differs per instance and per series, so each series of each instance grows at its own fixed multiple of the rate. Series of one instance are therefore not kept consistent with each other (for example, CPU time summed over states); use `share_counter` for series that must add up.
      With `bounded_gauge`, a gauge or a non-monotonic sum (such as a memory usage) varies around a fixed centre and always comes back to it, instead of wandering away from its seed. This is a simple model, not a reproduction of measured behaviour: the value of a series of an instance at a data point's timestamp `t` is `seed * k * (1 + variation * n(t))`, clamped to `[min, max]`:
        * `k` is a factor that is fixed per instance and series, log-uniformly distributed in `[1 / instance_spread, instance_spread]`.
        * `n(t)` is a smooth noise in `[-1, 1]`: the mean of three sinusoids with periods of `period`, `0.37 * period` and `0.11 * period`, each stretched by a factor in `[0.75, 1.25)` and shifted by a phase, both fixed per instance and series.
        * The value is therefore never further than `variation * seed * k` from its centre `seed * k`, and moves smoothly over about `period`.
        * The value is a function of the seed, the instance, the series and the time only: it keeps no state, does not depend on the other instances or on the order they are emitted in, and draws nothing from the random number generators.
        * `asInt` values are rounded to whole numbers and `asDouble` values to the most decimal places among the metric's seed values; a seed of `0` stays `0` (unless `min` is above it). `asInt` values beyond the int64 range are clamped to it, the same way on every architecture.
        Parameters, all optional:
        * `variation` (default `0.25`): the relative amplitude of the noise, in `[0, 1]`; `0` keeps every instance at its centre.
        * `period` (default `10m`): the longest period of the noise, a positive duration of at most `8760h`. Metrics with the same `variation_key` must have the same `period`.
        * `instance_spread` (default `2`): the spread of the centres of the instances, at least `1`; `1` centres every instance on the seed. Metrics with the same `variation_key` must have the same `instance_spread`.
        * `min`, `max`: bounds of the value; `min` must not be greater than `max`. For `asInt` values they are rounded inwards to whole numbers, and it is an error at startup if no whole number lies between them.
        * `variation_key` (default: the metric name): series of metrics with the same `variation_key` and the same data point attributes get the same `k` and the same noise, so they move together.
      With `share_gauge`, the series of a gauge or non-monotonic sum that differ only in the attribute named by `share_by` (for example the `state` of a memory usage) form a group that always adds up to a total, while each member's share varies around its seed share. This is again a simple model:
        * A group is all data points of one metric name in one resource and scope of the template whose attributes are equal apart from the `share_by` attribute. A group may have a single member, which then always equals the total.
        * The total is the `total` parameter, or the sum of the group's seed values. It is the same for every instance and at every time.
        * The mean share of member `s` for an instance is `f_s = seed_s * k_s / sum_j(seed_j * k_j)`, with `k` the per-instance factor described for `bounded_gauge`. Its share at a data point's timestamp `t` is `f_s * (1 + variation / 2 * (n_s(t) - sum_j(f_j * n_j(t))))`, with `n` the noise described for `bounded_gauge`, and its value is the total times its share.
        * The shares add up to `1`, each stays within `f_s * (1 ± variation * (1 - f_s))`, so it is never negative, and a seed of `0` stays `0`. The variation is capped at `0.999` in this formula, so a share never falls below `f_s / 1000`, even with `variation` `1`.
        * Every value is rounded (to whole numbers for `asInt`, to the most decimal places among the metric's seed values for `asDouble`, if any) and the rounding residual is added to the largest member, so the emitted members add up to the rounded total exactly for `asInt` and up to the last decimal for `asDouble`. No value becomes negative through this. `asInt` groups add up exactly up to a total of `2^53` (`9007199254740992`), the largest up to which a float64 holds every whole number; an `asInt` group with a larger total is an error at startup.
        * Like `bounded_gauge`, it keeps no state and draws nothing from the random number generators. A group is computed once per instance and interval.
        * Two groups of different metrics have the same shares at every time when all of these hold: the metrics have the same `variation_key`; the groups have the same attributes and the same members (values of the `share_by` attribute), with proportional seeds; and the metrics have the same `share_by`, `variation`, `period` and `instance_spread`. Members with equal attributes then get the same `k` and noise. The last condition is checked: `share_gauge` and `share_counter` metrics with the same `variation_key` that differ in one of these parameters are an error at startup. For example a memory usage by state divided by its total equals a memory utilization by state with `total` `1` (up to rounding).
        Parameters:
        * `share_by` (required): the name of the attribute that distinguishes the members of a group.
        * `total` (optional): what each group adds up to, at least `0`.
        * `variation` (default `0.25`), `period` (default `10m`), `instance_spread` (default `2`), `variation_key` (default: the metric name): as for `bounded_gauge`.
        At startup, it is an error if a data point lacks the `share_by` attribute, if two data points of a group have the same attributes, if a seed value is negative, if a group mixes `asInt` and `asDouble` values, or if `total` is above `0` for a group whose seeds are all `0`.
      With `share_counter`, the series of a cumulative monotonic sum form groups as for `share_gauge` (for example CPU time by `state`), and their rates add up to the group's total rate at every time, so the group's sum grows exactly at that rate while the split between members varies. Again a simple model:
        * Each member's seeded rate is its seed value divided by the time between its `startTimeUnixNano` and its `timeUnixNano`, with the same requirements as for `seeded_rate_counter`. The total rate `R` of a group is the sum of its members' seeded rates, and is the same for every instance.
        * The rate share of a member at time `t` is the `share_gauge` share, with the seeded rates in place of the seed values. Its value at a data point's timestamp `t` is `R` times the integral of its rate share from the data point's start timestamp `t0` to `t`, computed in closed form from the sinusoids.
        * The members therefore add up to `R * (t - t0)` at every sample (up to the rounding of each member to whole numbers for `asInt` or to the most decimal places among the metric's seed values for `asDouble`; no residual is moved between members), every member is monotonic, and a member with a seed of `0` stays `0`. With the variation capped at `0.999` (see `share_gauge`), a member's rate never falls below 1/1000 of its mean rate, which keeps it monotonic despite floating point error. The members of an `asInt` group are exact whole numbers up to `2^53`, and it is an error at startup if the group's seed values add up to more than that; above `2^53` a value is as precise as a float64, and values beyond the int64 range (after a very long time at a very high rate) are clamped to it, the same way on every architecture.
        * `t0` is the start timestamp of the sample: the receiver's `start_time`, or for an explicit instance its `start_timestamp` or latest restart. A sample right after a (re)start already reads what the group accumulated since then, without any per-instance state.
        * A `share_gauge` metric has the counter's instantaneous rate shares under the conditions under which two `share_gauge` metrics agree, with seeds proportional to the rates: the same `variation_key`, equal attributes and members, and the same `share_by`, `variation`, `period` and `instance_spread` (checked at startup). For example a CPU utilization by state with `total` `1` next to the CPU time by state.
        * The value does not depend on the shared template, on the other instances or on the random number generators.
        Parameters: `share_by` (required), `variation` (default `0.25`), `period` (default `10m`), `instance_spread` (default `2`) and `variation_key` (default: the metric name), as for `share_gauge`.
        The metric must be a cumulative monotonic `Sum`, also after `temporality_override`; a delta temporality is an error at startup.
      For example:
      ```yaml
      - name: system.memory.usage
        sum:
          aggregationTemporality: 2
          isMonotonic: false
          dataPoints:
            - { timeUnixNano: "0", asInt: "6000000000", attributes: [ { key: state, value: { stringValue: used } } ] }
            - { timeUnixNano: "0", asInt: "2000000000", attributes: [ { key: state, value: { stringValue: free } } ] }
        metadata:
          - { key: metricsgen.hint.class, value: { stringValue: share_gauge } }
          - { key: metricsgen.hint.share_by, value: { stringValue: state } }
          - { key: metricsgen.hint.variation_key, value: { stringValue: memory } }
      - name: system.cpu.time
        sum:
          aggregationTemporality: 2
          isMonotonic: true
          dataPoints:
            - { startTimeUnixNano: "1734307200000000000", timeUnixNano: "1734393600000000000", asDouble: 8640.25, attributes: [ { key: state, value: { stringValue: user } } ] }
            - { startTimeUnixNano: "1734307200000000000", timeUnixNano: "1734393600000000000", asDouble: 77759.75, attributes: [ { key: state, value: { stringValue: idle } } ] }
        metadata:
          - { key: metricsgen.hint.class, value: { stringValue: share_counter } }
          - { key: metricsgen.hint.share_by, value: { stringValue: state } }
          - { key: metricsgen.hint.variation_key, value: { stringValue: cpu } }
      - name: system.cpu.load_average.1m
        gauge:
          dataPoints:
            - { timeUnixNano: "0", asDouble: 1.5 }
        metadata:
          - { key: metricsgen.hint.class, value: { stringValue: bounded_gauge } }
          - { key: metricsgen.hint.variation, value: { stringValue: "0.4" } }
          - { key: metricsgen.hint.period, value: { stringValue: 30m } }
          - { key: metricsgen.hint.min, value: { stringValue: "0" } }
      ```
      The resource attributes template is used to simulate the individual instances.
      These resource attributes are injected into all resource metrics for which a matching resource attribute key exists.
      Supported placeholders:
        * `{{.InstanceID}}` (an integer equal to the number of the simulated instance, starting with `0`)
        * `{{.RandomIPv4}}`
        * `{{.RandomIPv6}}`
        * `{{.RandomMAC}}`
        * `{{.RandomHex <length>}}`
        * `{{.UUID}}`
        * `{{.InstanceStartTime}}`
        * `{{.RandomFrom <list>}}` (a random element from the list)
        * `{{.ModFrom <n> <list>}}` (an element from the list at index `n % len(list)`)
        * `{{.RandomIntn <n>}}` (a random integer in the range `[0, n)`)
        * `{{.Mod <x> <y>}}` (the result of `x % y`)
  * `churn`: allows to simulate instances spinning down and other instances taking their place, which will create new time series.
    This is a breaking change for existing configs that used the previous integer `churn` value.
    Replacement is deterministic and round-robin, so time series are replaced gradually instead of all at once.
    During startup, the initially created time series may have fewer samples because churn begins after the first emission.
    Configure either `samples_per_series` or `instance_lifetime`, but not both.
    * `samples_per_series`: the exact number of samples each new time series should emit before its instance is replaced.
      For example, with `samples_per_series: 6`, each new time series gets 6 samples.
      Must be greater than or equal to `1`.
    * `instance_lifetime`: the average lifetime of an active instance.
      Use this when you want to describe churn as a duration instead of a sample count.
      This does not guarantee an exact number of samples per time series.
      Must be greater than or equal to `interval`.
    Time series churn may have an impact on the performance of the backend.
  * `template_vars`: the `<path>.json`/`<path>.yaml` file is rendered as a template.
    This option lets you specify variables that are available during template rendering.
    This allows, for example, to simulate a variable number of network devices by generating metric data points with different attributes.
  * `temporality_override`: allows to override the temporality of the metrics in the scenario.
    Supported values: `cumulative`, `delta`.
    This can be used to accommodate backends that only support a specific temporality.
  * `histogram_override`: allows to override the histogram type for all histogram metrics in the scenario.
    Supported values: `exponential`.
    When set to `exponential`, all histogram metrics will be replaced with exponential histograms using the configured `exponential_histograms_template_path`. The series and their attributes are preserved.
  * `instances`: declares the instances of the scenario explicitly instead of generating `scale` instances from the resource attributes template.
    Each entry supports the following fields:
    * `resource_attributes`: resource attributes of the instance.
      They are applied to every resource in the metrics template: existing keys are overwritten and missing keys are added.
      Supported values are strings, booleans, integers, floating point numbers, lists, and maps.
      Keys must not be empty and values must not be null, also inside lists and maps.
    * `template_vars`: template variables of the instance.
      They are shallow-merged over the scenario's `template_vars` (the instance's value wins for a key set in both) and used to render the `<path>.json`/`<path>.yaml` file for this instance.
      For example, this lets each instance have its own set of network interfaces or disks.
    * `interval_offset`: the fixed offset within the `interval` at which the instance reports (such as `2.5s`), in `[0, interval)`.
      Takes precedence over `stagger_instances`; an explicit `0s` keeps the instance at the start of the interval while others are staggered.
      When unset, the offset is derived as described for `stagger_instances`, or `0` if it is not enabled.
    * `start_timestamp`: an RFC 3339 time (such as `2024-12-16T08:30:00Z`) reported as the start timestamp of every data point of the instance, instead of the receiver's `start_time`.
      For example, the time the host or process started.
      Defaults to the instance's `start_time` if that is set.
      Must not be after the instance's `start_time` if that is set, and otherwise not after the receiver's `start_time`.
    * `start_time` / `end_time`: RFC 3339 times that bound the lifetime of the instance.
      The instance only reports the samples whose time (the tick plus its interval offset, before jitter) is in `[start_time, end_time)`.
      When unset, the lifetime is unbounded on that side.
      `start_time` must be before `end_time`.
    * `restarts`: a list of RFC 3339 times, in strictly ascending order, at which the instance restarts.
      Each restart must be after `start_timestamp` and `start_time` (when set) and before `end_time` (when set).
    The start timestamp of a sample is the latest of the instance's start timestamp and its restarts at or before the sample time, or the receiver's `start_time` if there is none.
    When that start is after the receiver's `start_time` (the instance started or restarted inside the generated window), its counters count from zero at that start.
    The first sample after the start reads what each counter accumulated since the start at its rate over the latest interval: the increase of the shared template value from the previous tick to the sample's tick, times the time from the start to the sample time (tick plus interval offset, before jitter) divided by `interval`, rounded to a whole number for `asInt` values; the per-instance variation then applies as usual, with its time-dependent part also running from the start.
    It reads `0` only when the sample time equals the start. A start more than one `interval` before the first sample, such as a `start_timestamp` well before the instance's `start_time`, extrapolates that rate back to the start: the first sample reads what the series accumulated at its latest rate since the start, even when that is more than the shared template value at the sample's tick. Only absurd magnitudes are capped: before the per-instance variation, the first sample reads at most 2^62.
    From there, the series increase like those of an instance without the restart.
    Otherwise, the values stand for what accumulated before the window, as for any other instance: a `start_timestamp` before the receiver's `start_time` only sets the reported start timestamp, while a start or restart inside the window makes the counters count from zero at that time, as described above.
    Counters are the cumulative monotonic sums (after `temporality_override`) that have no generation hint or the hint `steady_counter`, `sparse_counter` or `seeded_rate_counter`.
    Sums with the hint `share_counter` count from the start timestamp of each sample by construction (see `share_counter`), so they also start from zero at a (re)start but need no baseline.
    Cumulative monotonic sums with any other hint (`current_count`, `slow_gauge`, `constant`, `clock`, `stable_binary`), gauges, non-monotonic sums and delta sums are not affected by restarts, and neither are histograms and exponential histograms.
    To count from zero, an instance that (re)starts inside the window keeps one value per counter series of its template from its first sample after the (re)start until its next restart or its `end_time`; no other instance keeps per-series state.
    A shared template used by such instances also keeps the value of each of its counter series before the latest interval, one value per series for the template, not per instance.
    An instance that is down for a while and comes back is declared as two entries with the same `resource_attributes` and non-overlapping lifetimes.
    The second entry is a different instance: as a repeat of the same definition (see below), it gets a different per-instance variation and, with `stagger_instances`, a different interval offset unless `interval_offset` is set.
    The `<path>-resource-attributes` template is optional when instances are declared.
    If it exists, it is rendered for each instance as for generated instances (with the merged variables as `.Vars`) and applied before `resource_attributes`.
    In that template, `{{.InstanceID}}` is the position of the instance in the list (starting with `0`) plus `instance_offset`; attributes rendered from it are the only part of the output that depends on the position.
    Instances whose merged template variables are equal, including their types (`1` and `1.0` are different), share one metrics template.
    Memory usage therefore grows with the number of distinct variable sets, not with the number of instances; if every instance has unique variables, each instance gets its own template.
    For every interval, the instances are emitted grouped by shared template: groups in order of the first instance that uses them, and instances in list order within a group.
    The output of a declared instance depends only on the receiver and scenario settings that apply to it (`seed`, `start_time`, `end_time`, `interval`, `interval_jitter_std_dev`, `stagger_instances`, `distribution`, `exponential_histograms_template_path`, and the scenario's `path`, `template_vars`, `temporality_override` and `histogram_override`) and on its own definition.
    It does not depend on its position in the list (except through `{{.InstanceID}}`), on the other instances, or on other scenarios and their order.
    A run over a subset of the instances, in any order and together with any other scenarios, therefore emits exactly the same data points, timestamps and resource attributes for each instance of the subset as a run over all of them.
    To get there, nothing about a declared instance is drawn from the random number generator that generated instances share:
    * Each instance has a key, a hash of the `seed`, the scenario's `path`, its merged template variables and its `resource_attributes`. The timing fields (`interval_offset`, `start_timestamp`, `start_time`, `end_time`, `restarts`) are not part of the key.
    * The shared template of a group advances with a random number generator of its own, seeded from the `seed`, the scenario's `path` and the group's merged template variables, so the template values of a group do not depend on which or how many instances use it.
    * The per-instance variation and the `stagger_instances` offset are derived from the key.
    * The resource attributes template is rendered with a random number generator seeded from the key, so placeholders such as `{{.RandomIPv4}}` or `{{.UUID}}` are stable for an instance.
    * The jitter and the values of explicit-bucket histograms are drawn from a random number generator seeded from the key and the tick.
    Several entries of one scenario with the same `resource_attributes` and the same merged template variables (compared by their JSON encoding here, so `1` and `1.0` count as equal) are repeats of one definition: the `n`-th repeat in list order mixes `n` into its key, so repeats differ from each other.
    The subset property holds for a repeat only if the subset keeps the same number of earlier repeats of its definition.
    The same definition declared in two scenarios with the same `path` (and the same merged template variables) has the same key, so both emit identical data.
    Template variables that differ only in a way their JSON encoding does not show (such as `1` and `1.0`) form separate templates, but those templates advance with the same random stream, and the instances count as repeats of one definition.
    In a configuration that mixes scenarios with declared instances and scenarios with generated instances, the generated ones emit the same values as without the scenarios with declared instances.
    With `stagger_instances`, their timestamps also stay the same only if each scenario with generated instances keeps its position in `scenarios`, as their offsets depend on it.
    Earlier versions drew the declared instances' values from the shared random number generator too, so the values of such generated scenarios differ from those versions.
    When instances are declared, `scale` is optional: when unset or `0`, all instances are simulated; otherwise the first `scale` instances are simulated, and `scale` must not exceed the number of instances.
    `concurrency` and `churn` are not supported with declared instances.
  * `instances_path`: the path of an NDJSON file that declares the instances of the scenario, one JSON object per line with the same fields as an entry of `instances`.
    Blank lines are ignored. Cannot be combined with `instances`.
    A relative path is resolved against the working directory of the collector, like `path`.
    The file is read when the receiver is created, so validating the configuration alone does not detect a missing or invalid file.

Example configuration:
```yaml
receivers:
  metricsgen:
    start_time: "2025-01-01T00:00:00Z"
    end_time: "2025-01-01T01:00:00Z"
    interval: 10s
    exit_after_end: true
    seed: 123
    scenarios:
      - path: builtin/hostmetrics
        scale: 100

exporters:
  nop:

service:
  pipelines:
    metrics:
      receivers: [metricsgen]
      exporters: [nop]
```

Explicitly declared instances, loaded from a file:
```yaml
receivers:
  metricsgen:
    start_time: "2025-01-01T00:00:00Z"
    end_time: "2025-01-01T01:00:00Z"
    interval: 10s
    seed: 123
    scenarios:
      - path: my-hosts
        instances_path: my-hosts.ndjson
```

`my-hosts.ndjson`:
```json
{"resource_attributes": {"host.name": "web-1", "host.ip": ["192.0.2.10", "2001:db8::10"]}, "template_vars": {"interfaces": ["lo", "eth0"]}, "interval_offset": "2.5s"}
{"resource_attributes": {"host.name": "db-1", "host.ip": ["192.0.2.20"], "cloud.provider": "example"}, "template_vars": {"interfaces": ["lo", "eth0", "eth1"]}, "start_timestamp": "2024-12-20T06:00:00Z", "restarts": ["2025-01-01T00:40:00Z"]}
{"resource_attributes": {"host.name": "web-2", "host.ip": ["192.0.2.11"]}, "template_vars": {"interfaces": ["lo", "eth0"]}, "start_time": "2025-01-01T00:15:00Z"}
```

`db-1` restarts after 40 minutes and its counters start again from zero; `web-2` only reports from the 15th minute on, counting from zero.

A snippet of `my-hosts.yaml` that emits one network series per interface and direction:
```yaml
          - name: system.network.io
            unit: By
            sum:
              aggregationTemporality: 2
              isMonotonic: true
              dataPoints:
                {{- range .interfaces }}
                - { asInt: "120000", attributes: [ { key: device, value: { stringValue: "{{ . }}" } }, { key: direction, value: { stringValue: receive } } ] }
                - { asInt: "80000", attributes: [ { key: device, value: { stringValue: "{{ . }}" } }, { key: direction, value: { stringValue: transmit } } ] }
                {{- end }}
```

The same instances can be declared inline:
```yaml
    scenarios:
      - path: my-hosts
        instances:
          - resource_attributes:
              host.name: web-1
              host.ip: ["192.0.2.10", "2001:db8::10"]
            template_vars:
              interfaces: [lo, eth0]
```

Real-time mode, running indefinitely:
```yaml
receivers:
  metricsgen:
    interval: 10s
    real_time: true
    run_indefinitely: true
    seed: 123
    scenarios:
      - path: builtin/hostmetrics
        scale: 100

exporters:
  nop:

service:
  pipelines:
    metrics:
      receivers: [metricsgen]
      exporters: [nop]
```
