# Rinha de Backend 2026

Implementacao em Rust + HAProxy para a Rinha de Backend 2026.

## Stack

- Rust sem dependencias externas no runtime.
- HAProxy apenas como load balancer.
- Indice vetorial customizado em `mmap` com vetores `int16`.

## Artefatos

```sh
cargo build --release
gzip -dc resources/references.json.gz | target/release/rinha-fraud build-index data/references.idx
```

## Execucao

```sh
docker compose up --build
```

Em Apple Silicon, para medir nativo `arm64` local:

```sh
scripts/k6-local-arm64.sh
```

## Validacao

```sh
cargo fmt --check
cargo test
cargo clippy -- -D warnings
docker compose config
docker compose -f submission/docker-compose.yml config
```

Melhor medicao local `arm64` neste Mac:

```txt
p99=1.44ms
http_errors=0
FP=0
FN=0
weighted_errors=0
final_score=5840.31
```

## Submissao

A imagem final deve ser publicada como `linux/amd64` contendo:

- `/app/data/references.idx`

O compose de submissao usa somente imagens publicas.
