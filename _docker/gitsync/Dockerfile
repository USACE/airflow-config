# BUILD GO BINARIES
FROM golang:1.23.1-alpine AS builder

# Create User; https://github.com/mhart/alpine-node/issues/48
# NOTE: Using same UID/GID as airflow user in container for shared volume convenience
RUN addgroup -g 50000 -S data \
  && adduser -u 50000 -S data -G data

# Install Git
RUN apk update && apk add --no-cache git ca-certificates
# Copy In Source Code
WORKDIR /go/src/app
COPY gitsync .
# Install Dependencies
RUN go get -d -v
# Build
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-w -s" -o /go/bin/gitsync

RUN mkdir /data && chown data:data /data && chmod 755 /data

#
# FROM scratch
# COPY --from=builder /go/bin/gitsync /go/bin/gitsync
# COPY --from=builder /etc/passwd /etc/passwd
# COPY --from=builder /data /data
# COPY --from=builder /etc/ssl/certs/ca-certificates.crt \
#                     /etc/ssl/certs/ca-certificates.crt

VOLUME "/data"

# https://medium.com/@lizrice/non-privileged-containers-based-on-the-scratch-image-a80105d6d341
USER data

ENTRYPOINT ["/go/bin/gitsync"]