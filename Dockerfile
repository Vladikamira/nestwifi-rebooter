# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/exporter ./cmd/rebooter

FROM gcr.io/distroless/static-debian12:nonroot AS exporter
COPY --from=build /out/exporter /exporter
USER 65532:65532
EXPOSE 9109
ENTRYPOINT ["/exporter"]

FROM gcr.io/distroless/static-debian12:nonroot AS rebooter
COPY --from=build /out/rebooter /rebooter
USER 65532:65532
EXPOSE 9109
ENTRYPOINT ["/rebooter"]
