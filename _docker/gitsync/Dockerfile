# BUILD GO BINARIES
FROM alpine:3.22.0

# Create User; https://github.com/mhart/alpine-node/issues/48
# NOTE: Using same UID/GID as airflow user in container for shared volume convenience
RUN addgroup -g 50000 -S data \
  && adduser -u 50000 -S data -G data

# Install Git
RUN apk update && apk add --no-cache git ca-certificates

WORKDIR /app

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER data

VOLUME "/data"

ENTRYPOINT ["/entrypoint.sh"]