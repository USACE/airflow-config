# BUILD GO BINARIES
# FROM alpine:3.23.2

# chainguard/wolfi-base - public image and more secure
FROM chainguard/wolfi-base:latest

# Create User; https://github.com/mhart/alpine-node/issues/48
# NOTE: Using same UID/GID as airflow user in container for shared volume convenience
RUN addgroup -g 50000 -S data \
  && adduser -u 50000 -S data -G data

# Install Git
RUN apk update && \
  apk add --no-cache git ca-certificates && \
  apk upgrade --no-cache

WORKDIR /app

COPY entrypoint.sh /entrypoint.sh

RUN mkdir /data && \
  chown data:data /data && \
  chmod 755 /data
  
# Infrastructure is still pointing to "/go/bin/gitsync"
  RUN chmod +x /entrypoint.sh && \
  mkdir -p /go/bin && \
  ln -s /entrypoint.sh /go/bin/gitsync


USER data

VOLUME "/data"

ENTRYPOINT ["/entrypoint.sh"]