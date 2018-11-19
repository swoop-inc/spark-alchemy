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

## For Spark users

- Native [HyperLogLog functions](../../wiki/Spark-HyperLogLog-Functions) that offer reaggregatable fast approximate distinct counting capabilities far beyond those in OSS Spark with interoperability to Postgres and even JavaScript.

## For Spark framework developers

- Helpers for [native function registration](../../wiki/Spark-Native-Functions)

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

Spark Alchemy is maintained by the team at [Swoop](https://www.swoop.com). If you'd like to contribute to our open-source efforts, by joining our team or from your company, let us know at `spark-interest at swoop dot com`.

## License

`spark-alchemy` is Copyright &copy; 2018 [Swoop, Inc.](https://www.swoop.com) It is free software, and may be redistributed under the terms of the LICENSE.
