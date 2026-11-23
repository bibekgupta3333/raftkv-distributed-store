# Multi-stage build of raftkvd (WBS 2.5.1). Targets:
#   release (default): static binary on scratch, non-root.
#   e2e: built with -tags debugpropose (debug Propose RPC), on Alpine with
#        iptables + iproute2 so the e2e fault driver can partition and delay
#        traffic (it runs those as root through `docker exec -u 0`; the node
#        process itself is still non-root).
# Both images also carry raftkv-observer (D-48); compose and k8s run it from
# the same image with the entrypoint overridden.
FROM golang:1.27-alpine AS build
ARG VERSION=dev
ARG SHA=unknown
ARG GOFLAGS_TAGS=""
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -tags "${GOFLAGS_TAGS}" \
      -ldflags "-s -w -X github.com/bibekgupta3333/raftkv-distributed-store/internal/version.Version=${VERSION} -X github.com/bibekgupta3333/raftkv-distributed-store/internal/version.GitSHA=${SHA}" \
      -o /out/ ./cmd/raftkvd ./cmd/raftkvctl ./cmd/raftkv-observer && \
    mkdir -p /rootfs/data && chown 65532:65532 /rootfs/data

FROM scratch AS release
COPY --from=build --chown=65532:65532 /rootfs/data /data
COPY --from=build /out/raftkvd /out/raftkvctl /out/raftkv-observer /usr/local/bin/
USER 65532:65532
VOLUME /data
EXPOSE 7000 9100
ENTRYPOINT ["/usr/local/bin/raftkvd"]
CMD ["--data-dir", "/data"]

FROM alpine:3.22 AS e2e
RUN apk add --no-cache iptables iproute2 && adduser -D -u 65532 raftkv && mkdir /data && chown 65532:65532 /data
COPY --from=build /out/raftkvd /out/raftkvctl /out/raftkv-observer /usr/local/bin/
USER 65532:65532
VOLUME /data
EXPOSE 7000 9100
ENTRYPOINT ["/usr/local/bin/raftkvd"]
CMD ["--data-dir", "/data"]
