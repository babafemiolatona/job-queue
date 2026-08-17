# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN go build -o /out/api ./cmd/api && \
    go build -o /out/worker ./cmd/worker && \
    go build -o /out/scheduler ./cmd/scheduler

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
ENTRYPOINT ["/usr/local/bin/api"]