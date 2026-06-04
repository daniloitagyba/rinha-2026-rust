# Teste local via WSL

Use WSL como caminho padrao para benchmark local com k6. Ele aproxima melhor o
ambiente local do teste remoto porque executa shell, Docker Compose, scripts `.sh`
e k6 dentro de um userspace Linux, reduzindo diferencas de PowerShell, paths,
montagens Windows e CRLF.

## Regra pratica

- Rode k6 local pelo WSL antes de comparar p99, score ou regressao.
- Use uma copia Linux em `/root/rinha-2026-rust-wsl-bench` para evitar ruido de
  I/O em `/mnt/c`.
- Normalize CRLF apenas nessa copia temporaria.
- Use um `PROJECT_NAME` novo por rodada quando quiser evitar interferencia de
  containers antigos.
- Leia sempre `test/results.json` gerado pela propria rodada.

## Pre-checagem

No PowerShell:

```powershell
wsl bash -lc 'docker info --format "{{.Architecture}} {{.OperatingSystem}}"'
```

O esperado nesta maquina e `x86_64 Docker Desktop`. A submissao continua mirando
`linux/amd64`.

## Rodada padrao

No PowerShell, a partir de qualquer pasta:

```powershell
wsl bash -lc 'set -eu; SRC=/mnt/c/dev/rinha-2026-rust; DST=/root/rinha-2026-rust-wsl-bench; mkdir -p "$DST"; rsync -a --delete --exclude .git --exclude target --exclude "test/results.json" --exclude "test/*.jsonl" "$SRC/" "$DST/"; cd "$DST"; sed -i "s/\r$//" scripts/*.sh docker/entrypoint.sh; mkdir -p /tmp/docker-anon; printf "{}\n" > /tmp/docker-anon/config.json; DOCKER_CONFIG=/tmp/docker-anon MODE=build PROJECT_NAME=rinha-rust-wsl-local ./scripts/k6-local.sh; cat test/results.json'
```

Esse comando:

- sincroniza o checkout Windows para uma pasta Linux temporaria;
- remove CRLF dos scripts shell somente na copia WSL;
- evita depender do credential helper do Docker Desktop dentro do WSL;
- sobe o compose de build, roda k6 e mostra o JSON de resultado.

## Baseline local

Use esta topologia como baseline local antes de comparar mudancas de transporte,
indice ou classificacao:

```powershell
wsl bash -lc 'cd /root/rinha-2026-rust-wsl-bench; mkdir -p /tmp/docker-anon; printf "{}\n" > /tmp/docker-anon/config.json; DOCKER_CONFIG=/tmp/docker-anon MODE=build PROJECT_NAME=rinha-rust-local ./scripts/k6-local.sh; cat test/results.json'
```

Registre o JSON gerado junto com commit, preset, CPU/memoria e se o teste foi
`MODE=build` ou `MODE=submission`.

## Rodada contra compose de submissao

Use este modo para validar a imagem/submissao local, sem rebuild do codigo:

```powershell
wsl bash -lc 'cd /root/rinha-2026-rust-wsl-bench; mkdir -p /tmp/docker-anon; printf "{}\n" > /tmp/docker-anon/config.json; DOCKER_CONFIG=/tmp/docker-anon MODE=submission PROJECT_NAME=rinha-rust-wsl-submission ./scripts/k6-local.sh; cat test/results.json'
```

Se a copia WSL ainda nao existir ou estiver desatualizada, rode primeiro a
rodada padrao.

## Presets

Para aproximar um cenario remoto Ryzen configurado no script:

```powershell
wsl bash -lc 'cd /root/rinha-2026-rust-wsl-bench; mkdir -p /tmp/docker-anon; printf "{}\n" > /tmp/docker-anon/config.json; DOCKER_CONFIG=/tmp/docker-anon MODE=build RUNNER_PRESET=remote-ryzen-hard PROJECT_NAME=rinha-rust-wsl-ryzen-hard ./scripts/k6-local.sh; cat test/results.json'
```

## Limpeza

O `scripts/k6-local.sh` derruba os servicos ao final por padrao. Se precisar
limpar manualmente:

```powershell
wsl bash -lc 'docker compose -p rinha-rust-wsl-local down --remove-orphans -v'
```

## Resultado de referencia

Para comparar rodadas, registre pelo menos:

- `p99`
- `final_score`
- `failure_rate`
- `false_positive_detections`
- `false_negative_detections`
- `http_errors`

Nao compare p99 de WSL, PowerShell e execucao remota como se fossem o mesmo
ambiente. Para decisao local, prefira sempre duas ou mais rodadas WSL.
