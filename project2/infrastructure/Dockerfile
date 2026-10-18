FROM golang:1.26.1-alpine3.23 AS builder
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/node ./cmd/node

FROM alpine:3.23.3
COPY --from=builder /out/node /app/node
EXPOSE 8080
ENTRYPOINT ["/app/node"]
