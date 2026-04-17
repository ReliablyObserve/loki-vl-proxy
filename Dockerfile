FROM golang:1.26.2-alpine3.22 AS builder
WORKDIR /app
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_TIME=unknown
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION} -X main.revision=${REVISION} -X main.buildTime=${BUILD_TIME}" -o /loki-vl-proxy ./cmd/proxy

FROM alpine:3.22.2
RUN apk --no-cache add ca-certificates
COPY --from=builder /loki-vl-proxy /usr/local/bin/loki-vl-proxy
EXPOSE 3100
ENTRYPOINT ["loki-vl-proxy"]
