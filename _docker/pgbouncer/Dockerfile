# Using https://github.com/brainsam/pgbouncer as a starting point
FROM alpine:3.19.1 AS build_stage

ARG PGBOUNCER_VERSION=1.21.0
ARG PGBOUNCER_DOWNLOAD_URL=https://github.com/pgbouncer/pgbouncer/releases/download/pgbouncer_1_21_0/pgbouncer-1.21.0.tar.gz

WORKDIR /
# python3, py3-pip, pip install docutils meets pandoc requirement for pgbouncer 
RUN apk --update add python3 py3-pip build-base automake autoconf libtool m4 libevent-dev openssl-dev c-ares-dev

# Prevent nag message from pip
RUN rm -rf /usr/lib/python3.11/EXTERNALLY-MANAGED

RUN pip install docutils \
    && wget ${PGBOUNCER_DOWNLOAD_URL} \
    && tar zxf pgbouncer-${PGBOUNCER_VERSION}.tar.gz \
    && rm pgbouncer-${PGBOUNCER_VERSION}.tar.gz \
    && cd /pgbouncer-${PGBOUNCER_VERSION}/ \
    && ./configure --with-cares --prefix=/pgbouncer \
    && make \
    && make install

WORKDIR /bin
RUN ln -s ../usr/bin/rst2man.py rst2man

FROM alpine:3.19.1

RUN apk --update add libevent openssl c-ares

RUN adduser --disabled-password pgbouncer \
    && mkdir -p /etc/pgbouncer /var/log/pgbouncer\
    && chown pgbouncer:pgbouncer /etc/pgbouncer /var/log/pgbouncer \
    && chmod 700 /etc/pgbouncer /var/log/pgbouncer
WORKDIR /
COPY --from=build_stage /pgbouncer /pgbouncer
COPY entrypoint.sh /entrypoint.sh
RUN chmod 750 /entrypoint.sh \
    && chown root:pgbouncer /entrypoint.sh
USER pgbouncer
ENTRYPOINT ["/entrypoint.sh"]
# ENTRYPOINT ["tail", "-f", "/dev/null"]