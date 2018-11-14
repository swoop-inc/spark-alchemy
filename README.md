# spark-alchemy

[![Download](https://api.bintray.com/packages/swoop-inc/maven/spark-alchemy/images/download.svg)](https://bintray.com/swoop-inc/maven/spark-alchemy/_latestVersion)

Spark Alchemy is a collection of open-source Spark tools & frameworks that have made the data engineering and
data science teams at [Swoop](https://www.swoop.com) highly productive in our demanding petabyte-scale environment
with rich data (thousands of columns).

## Installation

Add the following to your `libraryDependencies` in SBT:

```scala
resolvers += Resolver.bintrayRepo("swoop-inc", "maven")

libraryDependencies += "com.swoop" %% "spark-alchemy" % "<version>"
```

You can find all released versions [here](https://github.com/swoop-inc/spark-alchemy/releases).

## What you'll find here

### An extensive set of native HyperLogLog functions

See the [available HyperLogLog functions](https://github.com/swoop-inc/spark-alchemy/blob/master/alchemy/src/main/scala/com/swoop/alchemy/spark/expressions/hll/HLLFunctionRegistration.scala).

Precise distinct counts are expensive to compute because 

1. When large cardinalities are involved, precise distinct counts require a lot of memory/IO, and
2. Every row of data has to be processed for every query because distinct counts cannot be reaggregated

[HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) (HLL) can address both problems. Memory use is reduced via the tight binary sketch representation and the data can be pre-aggregated because HLL binary sketches can be merged. Useful background reading: [HLL in BigQuery](https://cloud.google.com/blog/products/gcp/counting-uniques-faster-in-bigquery-with-hyperloglog) and [HLL in Spark](https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html). It is unfortunate that Spark's HLL implementation does not expose the binary HLL sketches, which makes its usefulness rather limited: it addresses (1) but not (2) above.

The HLL Spark native functions in `spark-alchemy` provide two key benefits:

1. They expose HLL sketches as binary columns, enabling 1,000+x speedups in approximate distinct count computation via pre-aggregation.

2. They enable interoperability at the HLL sketch level with other data processing systems. We use an open-source [HLL library](https://github.com/aggregateknowledge/java-hll) with an independent [storage specification](https://github.com/aggregateknowledge/hll-storage-spec) and [built-in support for Postgres-compatible databases](https://github.com/citusdata/postgresql-hll) and even [JavaScript](https://github.com/aggregateknowledge/js-hll). This allows Spark to serve as a universal data (pre-)processing platform for systems that require fast query times, e.g., portals & dashboards.

### General utilities

- [Spark native function registration](https://github.com/swoop-inc/spark-alchemy/blob/master/alchemy/src/main/scala/com/swoop/alchemy/spark/expressions/NativeFunctionRegistration.scala) because there is no way to access this useful functionality from Spark's codebase.

## What's coming

- Configuration Addressable Production (CAP), Automatic Lifecycle Management (ALM) and Just-in-time Dependency Resolution
(JDR) as outlined in our Spark+AI Summit talk [Unafraid of Change: Optimizing ETL, ML, and AI in Fast-Paced Environments](https://databricks.com/session/unafraid-of-change-optimizing-etl-ml-ai-in-fast-paced-environments).

- Hundreds of productivity-enhancing extensions to the core user-level data types: `Column`, `Dataset`, `SparkSession`, etc.

- Data discovery and cleansing tools we use to ingest and clean up large amounts of dirty data from third parties.

- Cross-cluster named lock manager, which simplifies data production by removing the need for workflow servers much of the time.

- Versioned data source, which allows a new version to be written while the current version is being read.

- `case class` code generation from Spark schema, with easy implementation customization.

- Tools for deploying Spark ML pipelines to production.

- Lots more, as we are constantly building up our internal toolset.

## More from Swoop

- [spark-records](https://github.com/swoop-inc/spark-records): bulletproof Spark jobs with fast root cause analysis in the case of failures

## Community & contributing

Contributions and feedback of any kind are welcome. Please, create an issue and/or pull request.

Spark Records is maintained by the team at [Swoop](https://www.swoop.com). If you'd like to contribute to our open-source efforts, by joining our team or from your company, let us know at `spark-interest at swoop dot com`.

## License

`spark-alchemy` is Copyright &copy; 2018 [Swoop, Inc.](https://www.swoop.com) It is free software, and may be redistributed under the terms of the LICENSE.
