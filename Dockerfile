# syntax=docker/dockerfile:1

# ---- builder ----
FROM rust:1.95-bookworm AS builder
WORKDIR /src
COPY . .
# dashboard is compiled in (include_str!) — no Node stage. sqlx = runtime queries,
# so no DATABASE_URL is needed at build time.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release -p prospero-daemon --bin prosperod

# ---- runtime ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN useradd --uid 10001 --create-home --home-dir /home/app --shell /usr/sbin/nologin app
COPY --from=builder /src/target/release/prosperod /usr/local/bin/prosperod
ENV PROSPERO_ADDR=0.0.0.0:7878 \
    PROSPERO_DATA_DIR=/data
RUN mkdir -p /data && chown -R app:app /data /home/app
USER app
VOLUME ["/data"]
EXPOSE 7878
ENTRYPOINT ["prosperod"]
# default to standalone, no caliband in this image
CMD ["--no-autostart"]
