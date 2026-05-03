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
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

ENV BIND_ADDR=0.0.0.0:8080
ENV INDEX_PATH=/app/data/references.idx
ENV WORKERS=2
ENV KEEP_ALIVE_REQUESTS=256
ENV EARLY_CANDIDATES=16200
ENV MIN_CANDIDATES=16200
ENV MAX_CANDIDATES=32400
ENV PROFILE_FASTPATH=1
ENV PROFILE_MIN_COUNT=20
ENV EXACT_FALLBACK=risky
ENV FAST_PATH=false

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
