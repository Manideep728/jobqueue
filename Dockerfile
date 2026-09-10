# Multi-stage build: the toolchain (several hundred MB) stays in the builder and
# never ships. The final image carries only the three static binaries.

FROM golang:1.25-alpine AS builder
WORKDIR /src

# Copy the module files first and download dependencies as their own layer.
# Docker caches that layer, so editing Go source does not re-download modules.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a statically linked binary that runs on any base image,
# including scratch. -ldflags="-s -w" strips debug symbols to shrink it.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/queue-server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/queue-worker ./cmd/worker && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/queue ./cmd/queue

# Alpine rather than scratch, because "shell" jobs need a shell: the worker runs
# its payload through `sh -c`. It also brings ca-certificates and a real
# /etc/passwd for the non-root user below.
FROM alpine:3.20

# Workers execute arbitrary commands from the queue. Running as root would mean
# any submitted job is root on this container -- so drop to an unprivileged user.
RUN adduser -D -u 10001 queue

COPY --from=builder /out/queue-server /usr/local/bin/queue-server
COPY --from=builder /out/queue-worker /usr/local/bin/queue-worker
COPY --from=builder /out/queue        /usr/local/bin/queue

USER queue
EXPOSE 8080

# Overridden per service in docker-compose.yml.
CMD ["queue-server"]
