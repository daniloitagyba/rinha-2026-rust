# Rinha de Backend 2026

Implementacao em Rust + C para a Rinha de Backend 2026, focada em baixa
latencia no caminho HTTP oficial e deteccao sem erros no dataset local.

## Stack

- Rust sem dependencias externas no runtime.
- Load balancer C apenas de transporte, com fd-passing para as APIs.
- Servidor raw `epoll` nas APIs quando `BIND_ADDR=fd:*`.
- Indice vetorial customizado em `mmap` com vetores `int16`.

## Arquitetura

O compose sobe um load balancer C na porta `9999` e duas instancias da API Rust.
O LB aceita conexoes TCP, passa o file descriptor por sockets Unix de controle
e nao executa regra de antifraude. Com `FD_EPOLL_RAW=1`, a API recebe esses
descritores em um loop `epoll` proprio, sem converter a conexao para Tokio.

A API carrega `/app/data/references.idx` e classifica cada payload por:

- profile fast path derivado das referencias oficiais, sem usar massa de teste;
- busca aproximada no indice vetorial;
- fallback exato seletivo em regioes de maior risco/fronteira;
- poda semantica do fallback risky com grupos finos e raio configuravel.

O projeto nao usa `test-data.json`, `expected_approved`, IDs de transacao ou
artefatos de respostas como lookup no caminho de execucao.

Fast paths por perfil sao ligados ao fingerprint das referencias. Quando
`PROFILE_FASTPATH_REFERENCE_SHA256` nao bate com o hash gravado no indice, a API
desliga esses fast paths mesmo que `PROFILE_FASTPATH=1`.

## Indice

Para gerar o indice local a partir das referencias oficiais:

```sh
cargo build --release
scripts/build-index.sh resources/references.json.gz data/references.idx
```

## Execucao

Local com Docker Compose:

```sh
docker compose up --build
```

Smoke sem Docker, usando as referencias de exemplo:

```sh
scripts/smoke-local.sh
```

Smoke com Docker Compose:

```sh
scripts/smoke-compose.sh
```

Teste local proximo da engine, sempre mirando a topologia `linux/amd64`:

```powershell
.\scripts\k6-local.ps1
```

```sh
MODE=build sh scripts/k6-local.sh
```

## Validacao

```sh
cargo fmt --check
cargo test
cargo clippy -- -D warnings
docker compose config
docker compose -f submission/docker-compose.yml config
```

Para avaliar localmente contra o dataset oficial:

```sh
scripts/eval-official.sh
```

Esse script baixa `resources/references.json.gz` e `test/test-data.json` quando
necessario. O arquivo de teste e usado apenas pelo avaliador local, nao entra na
imagem final e nao e montado no compose de submissao.

O avaliador local aceita `EVAL_ERRORS_PATH` e `EVAL_DUMP_PATH` para investigar
FP/FN, vetor quantizado, contagem de fraudes do top-5 e caminho de decisao.

## Baseline local

Ultima medicao local validada em Docker Compose, usando `linux/amd64` e o
dataset oficial com 54.100 entradas:

```powershell
.\scripts\k6-local.ps1 -Mode build -ProjectName rinha-rust-local-final
```

Resultado em `test/results.json`:

- `p99=1.05ms`
- `final_score=5980.67`
- `failure_rate=0%`
- `false_positive_detections=0`
- `false_negative_detections=0`
- `http_errors=0`

Eval direto da imagem final:

- `accuracy=1.000000`
- `weighted_errors=0`
- `classify_latency_ns p99=530923`

Esse baseline e a referencia local para comparar com a execucao no servidor da
Rinha, onde o objetivo e medir a diferenca entre a cauda local e a cauda remota.

## Estrutura de branches

- `main`: codigo-fonte, Dockerfile, scripts, documentacao e arquivos de apoio.
- `submission`: somente os arquivos necessarios para executar o teste oficial,
  sem codigo-fonte.

A pasta `submission/` nesta branch `main` espelha o conteudo que deve existir na
raiz da branch `submission`:

```txt
docker-compose.yml
info.json
```

## Submissao

A imagem final deve ser publicada como `linux/amd64` contendo:

- `/app/data/references.idx`

O compose da branch `submission` usa somente imagens publicas e define
`platform: linux/amd64` para todos os servicos.

O procedimento para disparar e acompanhar o teste remoto esta em
[`REMOTE_TEST.md`](REMOTE_TEST.md).

O perfil padrao usa `FD_EPOLL_RAW=1`, `PROFILE_FASTPATH=1`,
`PROFILE_DOMINANT_FASTPATH=0`, `PROFILE_FASTPATH_REFERENCE_SHA256`,
`EXPECTED_REFERENCES_GZIP_SHA256`, `EARLY_EDGE_FALLBACK=1`,
`EXACT_FALLBACK=risky`, `RISKY_SEMANTIC_GROUPS=1`,
`RISKY_SEMANTIC_RADIUS=2`, `WORKERS=2` e
`EARLY_CANDIDATES/MIN_CANDIDATES/MAX_CANDIDATES=4500/4500/5500`.
`FAST_PATH=false` deixa as heuristicas manuais desligadas por padrao; o overload
fica desabilitado por padrao com `OVERLOAD_THRESHOLD=0`. O compose padrao
distribui CPU como `LB/API/API = 0.16/0.42/0.42`.
