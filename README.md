# Rinha de Backend 2026

Implementacao em Rust + HAProxy para a Rinha de Backend 2026.

## Stack

- Rust sem dependencias externas no runtime.
- HAProxy apenas como load balancer.
- Indice vetorial customizado em `mmap` com vetores `int16`.

## Arquitetura

O compose sobe um HAProxy na porta `9999` e duas instancias da API Rust. O
HAProxy encaminha as requisicoes para as APIs e nao executa regra de antifraude.

A API carrega `/app/data/references.idx` e classifica cada payload por:

- profile fast path derivado das referencias oficiais, sem usar massa de teste;
- busca aproximada no indice vetorial;
- fallback exato seletivo em regioes de maior risco/fronteira.

O projeto nao usa `test-data.json`, `expected_approved`, IDs de transacao ou
artefatos de respostas como lookup no caminho de execucao.

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

## Estrutura de branches

- `main`: codigo-fonte, Dockerfile, scripts, documentacao e arquivos de apoio.
- `submission`: somente os arquivos necessarios para executar o teste oficial,
  sem codigo-fonte.

A pasta `submission/` nesta branch `main` espelha o conteudo que deve existir na
raiz da branch `submission`:

```txt
docker-compose.yml
haproxy.cfg
info.json
```

## Submissao

A imagem final deve ser publicada como `linux/amd64` contendo:

- `/app/data/references.idx`

O compose da branch `submission` usa somente imagens publicas e define
`platform: linux/amd64` para todos os servicos.

O perfil padrao usa `PROFILE_FASTPATH=1`, `EXACT_FALLBACK=risky`,
`WORKERS=1` e `EARLY_CANDIDATES/MIN_CANDIDATES/MAX_CANDIDATES=16200/16200/32400`.
`FAST_PATH=false` deixa as heuristicas manuais desligadas por padrao.
