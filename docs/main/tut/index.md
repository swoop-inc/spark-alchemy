---
layout: home
title: "Home"
section: "Home"
---

## spark-alchemy

Spark Alchemy is a collection of open-source Spark tools & frameworks that have made the data engineering and
data science teams at [Swoop](https://www.swoop.com) highly productive in our demanding petabyte-scale environment
with rich data (thousands of columns).

We are preparing to release `spark-alchemy`. Click Watch above to be notified when we do.

Here is a preview of what we'd like to include here:

- Configuration Addressable Production (CAP), Automatic Lifecycle Management (ALM) and Just-in-time Dependency Resolution
(JDR) as outlined in our Spark+AI Summit talk [Unafraid of Change: Optimizing ETL, ML, and AI in Fast-Paced Environments](https://databricks.com/session/unafraid-of-change-optimizing-etl-ml-ai-in-fast-paced-environments).

- Our extensive set of [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) (HLL) functions that allow the saving of HLL sketches as binary columns for fast
reaggregation as well as HLL interoperability with Postgres. (Spark has an HLL implementation but does not expose the binary HLL sketches,
which makes its usefulness rather limited.)

- Hundreds of productivity-enhancing extensions to the core user-level data types: `Column`, `Dataset`, `SparkSession`, etc.

- Data discovery and cleansing tools we use to ingest and clean up large amounts of dirty data from third parties.

- Cross-cluster named lock manager, which simplifies data production by removing the need for workflow servers much of the time.

- Versioned data source, which allows a new version to be written while the current version is being read.

- `case class` code generation from Spark schema, with easy implementation customization.

- Tools for deploying Spark ML pipelines to production.

- Lots more, as we are constantly building up our internal toolset.

All this is code we use on a daily basis at [Swoop](https://www.swoop.com) and [IPM.ai](https://www.ipm.ai). However, making the code
suited for external use and taking on the responsibility to manage it for the broader Spark community is not a task we take lightly.
We are reviewing/refactoring APIs based on what we've learned from using the code over months and years at Swoop and adjusting it for
Spark 2.4.x. The process we go through is as follows:

1. Code we would like to consider for open-sourcing goes into an internal `spark-magic` library, which has no dependencies on Swoop-related
code, where it begins its "live use test" on multiple Spark clusters.

2. Once we feel the APIs have baked enough, we review candidate code and move it to an internal/private version of `spark-alchemy` to check
for dependencies and interactions with other components.

3. If all looks good, we promote the code to the public `spark-alchemy` (this repository).

If you'd like to contribute to our open-source efforts, by joining our team or from your company, let us know at `spark-interest at swoop dot com`.

For more Spark OSS work from Swoop, check out [spark-records](https://github.com/swoop-inc/spark-records).
