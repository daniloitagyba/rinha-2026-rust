# Rinha de Backend 2026

Implementacao em Rust + C para a Rinha de Backend 2026, focada em baixa
latencia no caminho HTTP oficial e deteccao sem erros no dataset local.

## Stack

- API em Rust.
- Load balancer em C com fd-passing para duas instancias da API.
- Servidor HTTP/1.1 raw com `epoll` nas APIs.
- Indice vetorial customizado em `mmap`, com buckets, vetores quantizados
  `int16` e fallback exato seletivo.
- Compose de submissao em `linux/amd64`.

## Arquitetura

O `lb` aceita conexoes TCP na porta `9999` e distribui os file descriptors por
sockets Unix de controle em `/sockets`. O load balancer nao executa logica de
fraude e nao usa dados do payload.

Cada API carrega `/app/data/references.idx` e classifica o payload com:

- fast path por perfil protegido pelo SHA das referencias;
- busca aproximada no indice vetorial;
- fallback exato seletivo em regioes de risco;
- grupos semanticos para reduzir o custo do fallback.

O caminho de execucao nao usa `test-data.json`, respostas esperadas, IDs de
transacao ou qualquer lookup derivado da massa de teste.

## Execucao Local

Smoke sem Docker:

```sh
scripts/smoke-local.sh
```

Smoke com Docker Compose:

```sh
scripts/smoke-compose.sh
```

Benchmark local mais proximo do remoto deve rodar no WSL com Docker Engine
nativo. O fluxo detalhado esta em [`LOCAL_TEST_WSL.md`](LOCAL_TEST_WSL.md).

## Validacao

```sh
cargo fmt --check
cargo test
docker compose config
docker compose -f submission/docker-compose.yml config
scripts/eval-official.sh
```

O avaliador local baixa as referencias e o dataset oficial quando necessario.
O dataset de teste e usado apenas fora da imagem final.

## Submissao

A branch `main` contem codigo, Dockerfile, scripts e documentacao. A branch
`submission` deve conter apenas:

```txt
docker-compose.yml
info.json
```

Antes de abrir teste remoto:

1. Publicar a imagem `linux/amd64` com `/app/data/references.idx` embutido.
2. Verificar que a tag SHA foi publicada no GHCR.
3. Atualizar a branch `submission` para usar a tag imutavel, nunca `latest`.
4. Abrir a issue oficial com `scripts/request-remote-test.ps1`.

O compose atual usa Docker bridge/default, fd-passing por volume tmpfs de
sockets, `LB/API/API = 0.20/0.40/0.40` CPU e `20MB/162MB/162MB` memoria.
