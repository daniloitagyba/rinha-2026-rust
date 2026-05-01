FROM rust:1.85-bookworm AS builder

WORKDIR /src
COPY . .

ARG ALLOW_EXAMPLE_INDEX=0
RUN cargo build --release
RUN mkdir -p /out/app /out/data
RUN cp target/release/rinha-fraud /out/app/rinha-fraud
RUN if [ "$ALLOW_EXAMPLE_INDEX" = "1" ] && [ -f resources/example-references.json ]; then \
      /out/app/rinha-fraud build-index /out/data/references.idx < resources/example-references.json ; \
    elif [ -f resources/references.json.gz ]; then \
      gzip -dc resources/references.json.gz | /out/app/rinha-fraud build-index /out/data/references.idx ; \
    fi

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /out/app/rinha-fraud /usr/local/bin/rinha-fraud
COPY --from=builder /out/data /app/data
COPY docker/entrypoint.sh /entrypoint.sh

ENV BIND_ADDR=0.0.0.0:8080
ENV INDEX_PATH=/app/data/references.idx
ENV WORKERS=1
ENV MIN_CANDIDATES=30000
ENV MAX_CANDIDATES=120000

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
