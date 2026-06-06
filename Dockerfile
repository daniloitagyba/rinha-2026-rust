FROM rust:1.85-bookworm AS builder

WORKDIR /src
COPY . .

ARG ALLOW_EXAMPLE_INDEX=0
RUN RUSTFLAGS="-C target-cpu=haswell -C target-feature=+avx2,+fma,+bmi2" cargo build --release
RUN mkdir -p /out/app /out/data
RUN cp target/release/rinha-fraud /out/app/rinha-fraud
RUN cc -O3 -DNDEBUG -march=haswell -mtune=haswell -o /out/app/rinha-lb src/lb/rinha-lb.c
RUN if [ "$ALLOW_EXAMPLE_INDEX" = "1" ] && [ -f resources/example-references.json ]; then \
      /out/app/rinha-fraud build-index /out/data/references.idx < resources/example-references.json ; \
    elif [ -f resources/references.json.gz ]; then \
      refs_sha="$(sha256sum resources/references.json.gz | awk '{print $1}')" ; \
      gzip -dc resources/references.json.gz | REFERENCES_GZIP_SHA256="$refs_sha" /out/app/rinha-fraud build-index /out/data/references.idx ; \
    fi

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /out/app/rinha-fraud /usr/local/bin/rinha-fraud
COPY --from=builder /out/app/rinha-lb /usr/local/bin/rinha-lb
COPY --from=builder /out/data /app/data
COPY docker/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

ENV BIND_ADDR=0.0.0.0:8080
ENV FD_CONTROL_SEQPACKET=1
ENV FD_EPOLL_RAW=1
ENV FD_EPOLL_BUSY_POLL_US=100
ENV FD_EPOLL_BUSY_POLL_BUDGET=8
ENV FD_EPOLL_PREFER_BUSY_POLL=1
ENV INDEX_PATH=/app/data/references.idx
ENV INDEX_HUGE=1
ENV INDEX_HUGEPAGES=1
ENV INDEX_MLOCK=1
ENV INDEX_REPORT_HUGEPAGES=1
ENV API_WARMUP_QUERIES=4096
ENV API_WARMUP_JITTER=512
ENV WORKERS=1
ENV KEEP_ALIVE_REQUESTS=256
ENV EARLY_CANDIDATES=9000
ENV MIN_CANDIDATES=9000
ENV MAX_CANDIDATES=18000
ENV PROFILE_FASTPATH=1
ENV PROFILE_FASTPATH_REFERENCE_SHA256=43d10de80609e77ce25740f375607afce7561ec44da50c27c142493db8fcab67
ENV EXPECTED_REFERENCES_GZIP_SHA256=43d10de80609e77ce25740f375607afce7561ec44da50c27c142493db8fcab67
ENV PROFILE_MIN_COUNT=15
ENV PROFILE_LEGIT_MIN_COUNT=15
ENV PROFILE_FRAUD_MIN_COUNT=200
ENV PROFILE_DOMINANT_FASTPATH=0
ENV PROFILE_DOMINANT_MIN_COUNT=15
ENV PROFILE_DOMINANT_MAX_OPPOSITE=2
ENV EXACT_FALLBACK=risky
ENV RISKY_SEMANTIC_GROUPS=1
ENV RISKY_SEMANTIC_RADIUS=2
ENV EARLY_EDGE_FALLBACK=1
ENV RISKY_AMOUNT_MIN=400
ENV RISKY_AMOUNT_MAX=3000
ENV RISKY_INSTALLMENTS_MIN=2200
ENV RISKY_INSTALLMENTS_MAX=6200
ENV RISKY_RATIO_MIN=850
ENV RISKY_KM_HOME_MIN=250
ENV RISKY_KM_HOME_MAX=4000
ENV RISKY_TX24H_MAX=5800
ENV RISKY_MERCHANT_AVG_MAX=420
ENV FAST_PATH=false

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
