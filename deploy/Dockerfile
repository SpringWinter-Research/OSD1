# The compiler image stays out of the published layers. sqlite and sqlite-vec
# compile C, so the slim Rust image still needs gcc.
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
ENV CARGO_INCREMENTAL=0
RUN cargo build --release -p osd1-server \
    && mv target/release/osd1 /osd1 \
    && rm -rf target /usr/local/cargo/registry /usr/local/cargo/git

FROM debian:bookworm-slim AS layout
RUN groupadd --gid 10001 osd1 \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin osd1 \
    && mkdir -p /data \
    && chmod 700 /data \
    && chown 10001:10001 /data \
    && apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && triplet="$(basename /lib/*-linux-gnu)" \
    && mkdir -p "/opt/libgcc/lib/${triplet}" \
    && cp -a "/lib/${triplet}/libgcc_s.so.1" "/opt/libgcc/lib/${triplet}/"

# The binary links libc, libm, and libgcc. base-nossl has libc and libm.
# libstdc++ and libssl are not linked, so they stay out of the image.
# COPY --chmod keeps /data at 700. A plain COPY resets a directory to 755.
FROM gcr.io/distroless/base-nossl-debian12
COPY --from=layout /etc/passwd /etc/passwd
COPY --from=layout /etc/group /etc/group
COPY --from=layout /opt/libgcc/lib/ /lib/
COPY --from=layout --chown=10001:10001 --chmod=700 /data /data
COPY --from=build /osd1 /usr/local/bin/osd1
USER 10001
ENV OSD1_DB_PATH=/data/osd1.db
ENV OSD1_HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
VOLUME /data
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=10 \
    CMD ["/usr/local/bin/osd1", "health"]
CMD ["osd1"]
