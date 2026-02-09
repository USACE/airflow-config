### BUILD STAGE
FROM chainguard/wolfi-base:latest AS build

ARG PGBOUNCER_VERSION=1.24.1
ARG PGBOUNCER_DOWNLOAD_URL=https://github.com/pgbouncer/pgbouncer/releases/download/pgbouncer_1_24_1/pgbouncer-1.24.1.tar.gz

WORKDIR /build

# Install build dependencies
RUN apk update && apk add --no-cache \
    build-base \
    automake \
    autoconf \
    libtool \
    m4 \
    wget \
    python3 \
    py3-docutils \
    libevent-dev \
    openssl-dev \
    c-ares-dev \
    ca-certificates

# Fetch and build pgbouncer
RUN wget ${PGBOUNCER_DOWNLOAD_URL} \
    && tar zxf pgbouncer-${PGBOUNCER_VERSION}.tar.gz \
    && cd pgbouncer-${PGBOUNCER_VERSION} \
    && ./configure \
        --with-cares \
        --prefix=/pgbouncer \
    && make \
    && make install

### RUNTIME STAGE
FROM chainguard/wolfi-base:latest

# Runtime deps only
RUN apk update && apk add --no-cache \
    libevent \
    openssl \
    c-ares \
    ca-certificates

# Create pgbouncer user
RUN addgroup -S pgbouncer \
    && adduser -S -G pgbouncer pgbouncer \
    && mkdir -p /etc/pgbouncer /var/log/pgbouncer \
    && chown pgbouncer:pgbouncer /etc/pgbouncer /var/log/pgbouncer \
    && chmod 700 /etc/pgbouncer /var/log/pgbouncer

# Copy compiled artifacts
COPY --from=build /pgbouncer /pgbouncer
COPY entrypoint.sh /entrypoint.sh

RUN chmod 750 /entrypoint.sh \
    && chown root:pgbouncer /entrypoint.sh

USER pgbouncer
WORKDIR /

ENTRYPOINT ["/entrypoint.sh"]
# ENTRYPOINT ["tail", "-f", "/dev/null"]