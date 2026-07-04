FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o tidal ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app/tidal /usr/local/bin/tidal
COPY --from=builder /app/config/config.yaml /etc/tidal/config.yaml

EXPOSE 8080
ENTRYPOINT ["tidal"]
