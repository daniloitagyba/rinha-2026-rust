# Rinha de Backend 2026

Implementacao em Rust + C para a Rinha de Backend 2026, focada em baixa
latencia no caminho HTTP oficial e deteccao sem erros no dataset local.

## Stack

- API em Rust.
- Servidor HTTP/1.1 raw em Rust no servico `lb`.
- Load balancer em C mantido no codigo para testes de transporte alternativos.
- Indice vetorial customizado em `mmap`, com buckets, vetores quantizados
  `int16` e fallback exato seletivo.
- Compose de submissao em `linux/amd64`.

## Arquitetura

O `lb` aceita conexoes TCP na porta `9999`, faz parse HTTP/JSON no caminho raw
com `epoll` e executa a classificacao no proprio processo para evitar custo de
proxy interno. `api1` e `api2` permanecem no compose como instancias minimas para
manter a topologia da submissao com tres servicos.

O processo ativo carrega `/app/data/references.idx` e classifica o payload com:

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

O compose atual usa Docker bridge/default. O servico `lb` executa o servidor raw
TCP e classifica no caminho quente; `api1` e `api2` permanecem como instancias
minimas. Os limites declarados sao `LB/API/API = 0.90/0.05/0.05` CPU e
`320MB/12MB/12MB` memoria.
