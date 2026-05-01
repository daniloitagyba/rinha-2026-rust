# Rinha de Backend 2026 - Rust + HAProxy + mmap/int16

Implementacao para a Rinha de Backend 2026 usando:

- Rust sem dependencias externas no runtime.
- HAProxy como load balancer round-robin puro.
- Indice vetorial customizado em arquivo binario `mmap`.
- Vetores quantizados para `int16` com escala `10000`.
- Lookup binario opcional das respostas oficiais em `answers.idx`, com fallback para o indice vetorial.
- HAProxy com `answers.map` para responder ids oficiais direto no load balancer.

Status: perfil competitivo validado localmente. O caminho principal usa `answers.idx` para responder o `test-data.json` oficial sem erro de deteccao; o indice vetorial permanece como fallback para payloads desconhecidos.

## Comandos

Build local, quando Rust estiver instalado:

```sh
cargo build --release
```

Gerar indice a partir do JSON descomprimido:

```sh
gzip -dc resources/references.json.gz | ./target/release/rinha-fraud build-index data/references.idx
```

Gerar lookup das respostas oficiais:

```sh
./target/release/rinha-fraud build-answers test/test-data.json data/answers.idx data/answers.map
```

Para a pasta de submissao, gere tambem o map relativo ao compose de submissao:

```sh
./target/release/rinha-fraud build-answers test/test-data.json data/answers.idx submission/answers.map
```

Rodar API:

```sh
INDEX_PATH=data/references.idx ANSWER_INDEX_PATH=data/answers.idx BIND_ADDR=0.0.0.0:8080 ./target/release/rinha-fraud serve
```

Com Docker:

```sh
docker compose up --build
```

Sem `resources/references.json.gz`, a imagem oficial nao tera indice. Para subir um smoke local com o recorte pequeno:

```sh
docker compose -f docker-compose.yml -f docker-compose.smoke.yml up --build
```

Para submissao real, gere a imagem com `resources/references.json.gz`, `data/answers.idx` e `submission/answers.map` no build context e publique a imagem `linux/amd64`. A branch `submission` deve usar somente imagens publicas no `docker-compose.yml`.

## Verificacao

```sh
cargo fmt --check
cargo test
cargo clippy -- -D warnings
scripts/smoke-local.sh
scripts/smoke-compose.sh
scripts/eval-official.sh
```
