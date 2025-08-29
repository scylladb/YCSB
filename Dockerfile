ARG VERSION
ARG INPUT_JAVA_VERSION=21
FROM eclipse-temurin:${INPUT_JAVA_VERSION}-jdk-noble AS build

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y maven \
    && ./scripts/package.sh "${VERSION}" "${INPUT_JAVA_VERSION}"

FROM eclipse-temurin:${INPUT_JAVA_VERSION}-jre-noble AS production

LABEL org.opencontainers.image.source="https://github.com/scylladb/YCSB"
LABEL org.opencontainers.image.title="ScyllaDB YCSB"

ENV YCSB_HOME="/usr/local/share/scylla-ycsb"
ENV PATH="$PATH:$YCSB_HOME/bin"
ENV LD_LIBRARY_PATH="/lib/x86_64-linux-gnu:/usr/local/lib:/usr/lib:/lib:/lib64:/usr/local/lib/x86_64-linux-gnu"
ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

WORKDIR /

RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime \
    && echo "$TZ" > /etc/timezone \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get purge -y \
    gcc make g++ apt-transport-https \
    autoconf bzip2 cpp libasan8 m4 libtirpc3 libtsan2 libubsan1 build-essential \
    pkg-config pkgconf pkgconf-bin build-essential \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN echo 'networkaddress.cache.ttl=0' >> $JAVA_HOME/lib/security/java.security
RUN echo 'networkaddress.cache.negative.ttl=0' >> $JAVA_HOME/lib/security/java.security
COPY --from=build /app/build ${YCSB_HOME}


SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENTRYPOINT ["ycsb.sh"]
