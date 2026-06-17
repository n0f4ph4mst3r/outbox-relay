FROM alpine:latest

RUN apk add --no-cache curl && \
    curl -fsSL https://github.com/pressly/goose/releases/latest/download/goose_linux_x86_64 -o /usr/local/bin/goose && \
    chmod +x /usr/local/bin/goose

COPY ./migrations /migrations

WORKDIR /

ENTRYPOINT ["goose", "-dir", "/migrations", "postgres"]