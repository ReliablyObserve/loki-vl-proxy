FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /loki-vl-proxy ./cmd/proxy

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
COPY --from=builder /loki-vl-proxy /usr/local/bin/loki-vl-proxy
EXPOSE 3100
ENTRYPOINT ["loki-vl-proxy"]
