FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/oqbridge ./cmd/oqbridge && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/oqbridge-migrate ./cmd/oqbridge-migrate

# ---

FROM alpine:3

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 oqbridge && \
    mkdir -p /var/lib/oqbridge && \
    chown oqbridge:oqbridge /var/lib/oqbridge

COPY --from=builder /out/oqbridge /usr/local/bin/oqbridge
COPY --from=builder /out/oqbridge-migrate /usr/local/bin/oqbridge-migrate

USER oqbridge
EXPOSE 9200

ENTRYPOINT ["oqbridge"]
CMD ["-config", "/etc/oqbridge/oqbridge.yaml"]
