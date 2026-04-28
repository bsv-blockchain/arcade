FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /arcade ./cmd/arcade

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
COPY --from=builder /arcade /usr/local/bin/arcade
ENTRYPOINT ["arcade"]
