# syntax=docker/dockerfile:1
# Multi-stage build (TECH-SPEC §1.1 container row): a static, trimmed binary
# built with the pinned Go toolchain, copied onto distroless/static nonroot —
# no shell, no package manager, non-root user.

FROM golang:1.27.1 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/kafka3o-gateway ./cmd/gateway

# Digest-pinned (TECH-SPEC §1.0: reproducible images); Renovate bumps it.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=build /out/kafka3o-gateway /kafka3o-gateway
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/kafka3o-gateway"]
