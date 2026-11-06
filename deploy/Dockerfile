FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN cargo build --release -p osd1-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 osd1 \
    && mkdir -p /data \
    && chown osd1:osd1 /data
COPY --from=build /src/target/release/osd1 /usr/local/bin/osd1
USER osd1
ENV OSD1_DB_PATH=/data/osd1.db
ENV OSD1_HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
VOLUME /data
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=10 \
    CMD wget -q -O - http://127.0.0.1:8080/health || exit 1
CMD ["osd1"]
