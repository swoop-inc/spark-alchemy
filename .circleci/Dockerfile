# spark-alchemy CircleCI custom image
#
# This custom image is built following CircleCI instructions ( https://circleci.com/docs/2.0/custom-images/ ),
# using the adoptopenjdk docker images as a base, and installing other custom app/tools.
#
# https://github.com/AdoptOpenJDK/openjdk-docker/blob/4dd8db3c0fb30281ead0baa706800a605d3ebbe1/8/jdk/ubuntu/Dockerfile.hotspot.releases.full
# https://github.com/CircleCI-Public/circleci-dockerfiles/blob/master/openjdk/images/8u222-jdk-stretch/Dockerfile

FROM adoptopenjdk:8u222-b10-jdk-hotspot-bionic

# make Apt non-interactive
RUN echo 'APT::Get::Assume-Yes "true";' > /etc/apt/apt.conf.d/90circleci \
  && echo 'DPkg::Options "--force-confnew";' >> /etc/apt/apt.conf.d/90circleci

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    git mercurial xvfb apt \
    locales sudo openssh-client ca-certificates tar gzip parallel \
    net-tools netcat unzip zip bzip2 gnupg curl wget make

# Set timezone to UTC by default
RUN ln -sf /usr/share/zoneinfo/Etc/UTC /etc/localtime

# Use unicode
RUN update-locale LANG="en_US.UTF-8"
ENV LANG=en_US.UTF-8

RUN groupadd --gid 3434 circleci \
  && useradd --uid 3434 --gid circleci --shell /bin/bash --create-home circleci \
  && echo 'circleci ALL=NOPASSWD: ALL' >> /etc/sudoers.d/50-circleci \
  && echo 'Defaults    env_keep += "DEBIAN_FRONTEND"' >> /etc/sudoers.d/env_keep

# BEGIN IMAGE CUSTOMIZATIONS

# Install openjfx
RUN apt-get install -y --no-install-recommends openjfx

# Install sbt from https://circle-downloads.s3.amazonaws.com/circleci-images/cache/linux-amd64/sbt-latest.tgz
RUN curl --silent --show-error --location --fail --retry 3 --output /tmp/sbt.tgz https://circle-downloads.s3.amazonaws.com/circleci-images/cache/linux-amd64/sbt-latest.tgz   && tar -xzf /tmp/sbt.tgz -C /opt/   && rm /tmp/sbt.tgz   && /opt/sbt/bin/sbt sbtVersion

# Update PATH for Java tools
ENV PATH="/opt/sbt/bin:$PATH"

# smoke test with path
RUN sbt sbtVersion

# Add build-essential
RUN apt-get install -y --no-install-recommends build-essential

RUN apt-get install -y --no-install-recommends ruby-full rubygems autogen autoconf libtool make \
  && gem update --system \
  && gem env \
  && gem install sass jekyll --no-document

# END IMAGE CUSTOMIZATIONS

RUN rm -rf /var/lib/apt/lists/*

USER circleci
ENV PATH /home/circleci/.local/bin:/home/circleci/bin:${PATH}

CMD ["/bin/bash"]
