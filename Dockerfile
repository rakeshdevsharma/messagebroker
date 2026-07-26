FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/broker ./cmd/broker

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/broker /usr/local/bin/broker
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/broker"]
