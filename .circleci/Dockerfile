# spark-alchemy CircleCI custom image
#
# This custom image is built following CircleCI instructions ( https://circleci.com/docs/2.0/custom-images/ ),
# using the adoptopenjdk docker images as a base, and installing other custom app/tools.
#
# https://github.com/AdoptOpenJDK/openjdk-docker/blob/4dd8db3c0fb30281ead0baa706800a605d3ebbe1/8/jdk/ubuntu/Dockerfile.hotspot.releases.full
# https://github.com/CircleCI-Public/circleci-dockerfiles/blob/master/openjdk/images/8u222-jdk-stretch/Dockerfile

FROM adoptopenjdk/openjdk8:jdk8u222-b10-alpine-slim

RUN echo @edge http://nl.alpinelinux.org/alpine/edge/community >> /etc/apk/repositories \
    && echo @edge http://nl.alpinelinux.org/alpine/edge/main >> /etc/apk/repositories \
    && echo @edge http://nl.alpinelinux.org/alpine/edge/testing >> /etc/apk/repositories \
    && apk upgrade --update-cache --available

ENV RUBYOPT="W0" \
  LANG=en_US.UTF-8 \
  LANGUAGE=en_US.UTF-8

ENV BUILD_PACKAGES=" \
  build-base \
  curl-dev \
  gdbm \
  harfbuzz@edge \
  icu-dev \
  libc-dev \
  libffi-dev \
  libxml2-dev \
  libxslt-dev \
  linux-headers \
  nss@edge \
  ruby-dev \
  tzdata \
  zlib-dev \
  "

ENV RUNTIME_PACKAGES="\
  bash \
  ca-certificates \
  curl \
  dockerize@edge \
  file \
  git \
  grep \
  gzip \
  less \
  libcurl \
  libffi \
  libgcrypt \
  ncurses \
  netcat-openbsd \
  openssh \
  openssl \
  parallel \
  ruby \
  sudo \
  tar \
  unzip \
  wget \
  which \
  zip \
  "

RUN apk --update --upgrade --no-cache add $BUILD_PACKAGES $RUNTIME_PACKAGES

# Set timezone to UTC by default
RUN ln -sf /usr/share/zoneinfo/Etc/UTC /etc/localtime

RUN addgroup --gid 3434 circleci \
  && adduser --uid 3434 --ingroup circleci --shell /bin/bash --disabled-password circleci \
  && echo 'circleci ALL=NOPASSWD: ALL' >> /etc/sudoers.d/50-circleci

# BEGIN IMAGE CUSTOMIZATIONS

# Not sure why but jekyll requires "json" and "bigdecimal" to be installed manually
RUN gem update --system --no-document \
  && gem env \
  && gem install sass jekyll json bigdecimal --no-document

# smoke test with path
RUN sass --version \
  && jekyll --version

# Install sbt from https://circle-downloads.s3.amazonaws.com/circleci-images/cache/linux-amd64/sbt-latest.tgz
RUN curl --silent --show-error --location --fail --retry 3 --output /tmp/sbt.tgz https://circle-downloads.s3.amazonaws.com/circleci-images/cache/linux-amd64/sbt-latest.tgz \
  && tar -xzf /tmp/sbt.tgz -C /opt/ \
  && rm /tmp/sbt.tgz \
  && /opt/sbt/bin/sbt sbtVersion

# Update PATH for Java tools
ENV PATH="/opt/sbt/bin:$PATH"

# smoke test with path
RUN sbt sbtVersion

# Clean up
RUN gem cleanup \
  && rm -rf /usr/lib/ruby/gems/*/cache/* \
        /var/cache/apk/* \
        /tmp/* \
        /var/tmp/*

# END IMAGE CUSTOMIZATIONS

USER circleci
WORKDIR /home/circleci
ENV PATH /home/circleci/.local/bin:/home/circleci/bin:${PATH}

CMD ["/bin/bash"]
