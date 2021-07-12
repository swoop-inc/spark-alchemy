# spark-alchemy

Spark Alchemy is a collection of open-source Spark tools & frameworks that have made the data engineering and
data science teams at [Swoop](https://www.swoop.com) highly productive in our demanding petabyte-scale environment
with rich data (thousands of columns).

## Supported languages

While `spark-alchemy`, like Spark itself, is written in Scala, much of its functionality, such as interoperable HyperLogLog functions, can be used from other Spark-supported languages such as SparkSQL and [Python](#for-python-developers). 

## Installation

Add the following to your `libraryDependencies` in SBT:

```scala
libraryDependencies += "com.swoop" %% "spark-alchemy" % "1.0.1"
```

You can find all released versions [here](https://repo1.maven.org/maven2/com/swoop/spark-alchemy_2.12/).

Some use cases such as interoperability with PySpark may require the assembly of a fat JAR of `spark-alchemy`. To assemble, run `sbt assembly`. To skip tests during assembly, run `sbt 'set sbt.Keys.test in assembly := {}' assembly` instead.

## For Spark users

- Native [HyperLogLog functions](../../wiki/Spark-HyperLogLog-Functions) that offer reaggregatable fast approximate distinct counting capabilities far beyond those in OSS Spark with interoperability to Postgres and even JavaScript. Just as Spark's own native functions, once the functions are registered with Spark, they can be used from SparkSQL, Python, etc.

## For Spark framework developers

- Helpers for [native function registration](../../wiki/Spark-Native-Functions)

- Look at [`SparkSessionSpec`](alchemy/src/test/scala/com/swoop/test_utils/SparkSessionSpec.scala) as an example of how to reuse advanced Spark testing functionality from OSS Spark.

## For Python developers

- See [HyperLogLog functions](../../wiki/Spark-HyperLogLog-Functions) for an example of how `spark-alchemy` HLL functions can be registered for use through PySpark.

## What we hope to open source in the future

- Configuration Addressable Production (CAP), Automatic Lifecycle Management (ALM) and Just-in-time Dependency Resolution
(JDR) as outlined in our Spark+AI Summit talk [Unafraid of Change: Optimizing ETL, ML, and AI in Fast-Paced Environments](https://databricks.com/session/unafraid-of-change-optimizing-etl-ml-ai-in-fast-paced-environments).

- Utilities that make [Delta Lake](https://delta.io) development substantially more productive.

- Hundreds of productivity-enhancing extensions to the core user-level data types: `Column`, `Dataset`, `SparkSession`, etc.

- Data discovery and cleansing tools we use to ingest and clean up large amounts of dirty data from third parties.

- Cross-cluster named lock manager, which simplifies data production by removing the need for workflow servers much of the time.

- `case class` code generation from Spark schema, with easy implementation customization.

- Tools for deploying Spark ML pipelines to production.

## Development

Build docs microsite

```sbt "project docs" makeMicrosite```

Run docs microsite locally (run under `docs/target/site` folder)

```
jekyll serve -b /spark-alchemy
``` 

[More details](https://47degrees.github.io/sbt-microsites/)

## More from Swoop

- [spark-records](https://github.com/swoop-inc/spark-records): bulletproof Spark jobs with fast root cause analysis in the case of failures

## Community & contributing

Contributions and feedback of any kind are welcome. Please, create an issue and/or pull request.

Spark Alchemy is maintained by the team at [Swoop](https://www.swoop.com). If you'd like to contribute to our open-source efforts, by joining our team or from your company, let us know at `spark-interest at swoop dot com`.

## License

`spark-alchemy` is Copyright &copy; 2018-2020 [Swoop, Inc.](https://www.swoop.com) It is free software, and may be redistributed under the terms of the LICENSE.
