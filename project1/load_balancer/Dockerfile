FROM golang:1.26.5-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o main .

FROM alpine:3.18.4 AS runner
WORKDIR /app
COPY --from=builder /app/main .
RUN chmod +x ./main
ARG STRATEGY=round-robin
ENV STRATEGY=$STRATEGY
CMD ["sh", "-c", "./main \"$STRATEGY\""]
