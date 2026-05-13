FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY bin/cdc-linux-arm64 /cdc

ENTRYPOINT ["/cdc"]
