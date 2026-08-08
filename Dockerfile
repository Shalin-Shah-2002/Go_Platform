FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/order-service ./cmd/order-service
RUN CGO_ENABLED=0 go build -o /out/worker-service ./cmd/worker-service

FROM alpine:3.20
RUN adduser -D -H appuser
COPY --from=build /out/order-service /order-service
COPY --from=build /out/worker-service /worker-service
USER appuser
EXPOSE 8080
ENTRYPOINT ["/order-service"]
