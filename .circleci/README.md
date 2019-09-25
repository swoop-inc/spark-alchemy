# Swoop's CircleCI Custom-Built Docker Image

<https://circleci.com/docs/2.0/custom-images/>

## Prerequisites

* You must be logged into docker hub (docker login) and have access to the [swoopinc](https://cloud.docker.com/u/swoopinc/) organization to update the build container.
* Docker tags should be named: `[base-image]-circleci-[datetime-stamp]`, e.g. `adoptopenjdk-8u222-circleci-201909250040`

## Instructions

1. Build the docker image:

    ```
    docker build -t swoopinc/spark-alchemy:adoptopenjdk-8u222-circleci-$(date +%Y%m%d%H%M) .
    ```

1. Deploy the docker image to Docker Hub:

    ```
    docker push swoopinc/spark-alchemy:<tag-from-previous-step>
    ```
