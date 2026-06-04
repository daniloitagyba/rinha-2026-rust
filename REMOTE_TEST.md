# Teste remoto

O disparo do teste remoto abre uma issue no repositorio oficial com titulo e
corpo `rinha/test <submission>`.

Neste projeto, o nome da submissao e:

```txt
itagyba-rust
```

## Criar e aguardar

```powershell
.\scripts\request-remote-test.ps1
```

O script cria a issue em `zanfranceschi/rinha-de-backend-2026`, consulta o
resultado em `results-preview.json` e grava a resposta em
`test/remote-result.json` quando o resultado for publicado.

## Apenas criar a issue

```powershell
.\scripts\request-remote-test.ps1 -CreateOnly
```

Use esse modo quando a intencao for apenas entrar na fila remota e acompanhar o
resultado depois.

## Aguardar uma issue existente

```powershell
.\scripts\request-remote-test.ps1 -NoCreate -IssueUrl https://github.com/zanfranceschi/rinha-de-backend-2026/issues/<numero-da-issue>
```
