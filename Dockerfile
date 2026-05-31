# ---- Build stage ----
FROM golang:1.23-alpine AS builder

WORKDIR /src

# Cache dependencies separately from source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a static binary — no CGO, no external runtime deps
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /broker ./cmd/broker

# ---- Runtime stage ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

# Non-root user for security
RUN addgroup -S kafka && adduser -S kafka -G kafka

WORKDIR /app
COPY --from=builder /broker /app/broker

# Data directory — mounted as a named volume in docker-compose
RUN mkdir -p /data && chown kafka:kafka /data

USER kafka

EXPOSE 9092

ENTRYPOINT ["/app/broker"]
CMD ["--addr", ":9092", "--data-dir", "/data", "--node-id", "1", "--host", "broker-1"]
