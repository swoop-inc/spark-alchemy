# Release Process

1. Develop new code on feature branches.

1. After development, testing, and code review, merge changes into the `master` branch.

1. When ready to deploy a new release, merge and push changes from `master` to the `release` branch. Travis-CI will then:

    * Build the project
    * Run tests
    * Deploy artifacts to Bintray
    * Publish the microsite to Github Pages
    * Create a new release on the [Github Project Release Page](https://github.com/swoop-inc/spark-alchemy/releases)

## Project Version Numbers

* The `VERSION` file in the root of the project contains the version number that SBT will use for the `spark-alchemy` project.
* The format should follow [Semantic Versioning](https://semver.org/) with the patch number matching the Travis CI build number when deploying new releases.
* During deployment, Travis CI will read the MAJOR and MINOR version numbers from the `VERSION` file, but substitute the build number into the PATCH portion. In other words, if project developers wish to change the MAJOR or MINOR version numbers of the `spark-alchemy` project, they can simply change them in the `VERSION` file.
* During local development and when checked into Git, the version number defined in the `VERSION` file should end with the `-SNAPSHOT` string.
