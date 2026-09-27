FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mock-provider ./cmd/mock-provider

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 notifier \
    && adduser -S -D -H -u 10001 -G notifier notifier \
    && mkdir -p /data /etc/notifier \
    && chown -R notifier:notifier /data /etc/notifier

COPY --from=build /out/notifier /usr/local/bin/notifier
COPY --from=build /out/mock-provider /usr/local/bin/mock-provider
COPY configs/config.docker.yaml /etc/notifier/config.yaml

USER notifier
VOLUME ["/data"]
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/notifier"]
CMD ["-config", "/etc/notifier/config.yaml"]
