# Architecture — Jungle Gaming Backend Challenge (Go)

Documento exigido pelo README §15. Descreve exclusivamente a arquitetura
implementada neste repositório. Decisões, limitações e trabalho não
concluído estão explicitados na última seção.

## 1. Visão geral

Serviço em Go (Uber Fx) que processa operações financeiras de provedores de
jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`, mais `OPENING` interno)
por HTTP e por SQS, com garantias equivalentes nos dois canais.

```text
Provider --HTTP--> mux (net/http) --OIDC--> WagerService/WalletService --tx--> PostgreSQL
Keycloak --JWKS--> auth.Verifier (descoberta OIDC + cache 5min)

SQS wager-input --> Consumer --> Inbox + WagerService --tx--> PostgreSQL --ack/delete-->
WagerService --tx--> outbox_events --> Publisher --> SQS wager-events
                                                   └-> esgotados --> SQS wager-events-dlq
```

Pacotes: `cmd/server` (só monta `fx.New(app.Module).Run()`),
`internal/app` (composição Fx e lifecycle), `internal/transport/http`
(handlers + middlewares, sem regra financeira), `internal/application`
(casos de uso), `internal/domain` (entidades, sem I/O), 
`internal/repository/postgres` (pgx com SQL explícito),
`internal/messaging/sqs` (consumer + publisher), `internal/auth` (OIDC),
`internal/config`, `internal/observability` (métricas próprias, logs,
health). O domínio não importa Fx, HTTP, SQS nem pgx (há teste estrutural
disso em `internal/transport/http`).

## 2. Dinheiro

`domain.Money`: `int64` em centavos + moeda ISO-3 em maiúsculas. Nenhum
`float32/64` em parsing (`NewMoneyFromDecimal` dígito a dígito, exige
exatamente 2 casas), aritmética (`Add`/`Sub` com `ErrOverflow` e
`ErrCurrencyMismatch`), serialização (`AmountString`) ou persistência
(`BIGINT` de centavos). `Sub` recusa resultado negativo
(`ErrInsufficientFunds`); `Negate()` existe só para diferenças/cálculos
internos — saldo de carteira nunca é negativo (constraint
`wallets_balance_non_negative`).

## 3. HTTP e contratos

`net/http` com `ServeMux` e `PathValue` (`internal/app/app.go` registra as
rotas). Endpoints: `POST /wallets` (abertura, serviço interno),
`GET /wallets/{id}`, `GET /wallets/{id}/ledger` (cursor opaco
`base64(created_at|id)`, ordenação estável `(created_at, id)`),
`POST /wagering/transactions` (header `Idempotency-Key` obrigatório; o
servidor nunca substitui a chave recebida), 
`GET /wagering/transactions/{id}`,
`GET /providers/{providerId}/wagering/transactions/{externalId}`,
`POST /wallets/{id}/reconciliation` (serviço interno, sempre 200 com
`consistent` + `issues: []` nunca nulo), `GET /health/live|ready`,
métricas Prometheus (sem OIDC).

## 4. Serviços de aplicação e fronteiras de transação

`WagerService.ProcessWager` executa, numa única transação PostgreSQL:
lock da carteira (`SELECT ... FOR UPDATE`), validações de domínio,
decisão (PROCESSED/REJECTED/FAILED/PENDING_REFERENCE), movimento de saldo,
`wallet_ledger_entries`, `wager_transactions` e eventos da outbox
(decisão + `WalletBalanceChanged` quando há movimento). `LOSS` produz
`WagerTransactionProcessed` sem ledger e sem `WalletBalanceChanged`.
`WalletService.OpenWallet` cria carteira + `OPENING` + ledger + outbox no
mesmo commit (saldo zero: sem `OPENING`/ledger/eventos; conflito de
`(player_id, currency)` vira `ErrWalletAlreadyExists` via constraint, sem
check-then-create). `ReconcileWallet` é somente leitura (dois `SELECT`s,
sem transação): reconstrói o saldo do ledger e compara com a carteira.

## 5. PostgreSQL, ledger e concorrência

pgx com SQL explícito; locks e constraints explícitos e verificáveis.
Coordenação por carteira via `SELECT ... FOR UPDATE` na linha da wallet
dentro da transação da operação — carteiras distintas avançam em paralelo,
sem lock global. Lost updates são impedidos pelo lock de linha; a coluna
`version` (inicial 1) acompanha mudanças de saldo. `wallets`:
`UNIQUE(player_id, currency)`, `balance >= 0`. `wallet_ledger_entries`:
`UNIQUE(wallet_id, transaction_id)`, `amount > 0`,
`balance_before/balance_after >= 0`, FKs para wallet e transação; o
repositório não expõe update/delete E o trigger
`wallet_ledger_entries_no_update_delete` (migration 006) rejeita
UPDATE/DELETE no banco — correções exigem novos lançamentos. Limpeza de
testes usa `TRUNCATE` (manutenção operacional, fora do caminho da app).
`wager_transactions`: `UNIQUE(provider_id, external_transaction_id)` e
`UNIQUE(provider_id, idempotency_key)`, kinds/status oficiais e referência
obrigatória para reversões (migration 002). Migrations 001–006 aplicadas
em ordem com `psql -f` (procedimento nos arquivos e no `migrate.sh` do
Compose, idempotente via `schema_migrations`); reversões documentadas nos
próprios arquivos.

## 6. Idempotência

Persistente no banco, sobrevive a reinícios. HTTP: chave do header
`Idempotency-Key` + identidade `(providerId, externalTransactionId)`;
chave reutilizada com conteúdo diferente gera conflito; replay devolve o
`resultingBalance` do processamento original. SQS: `data.idempotencyKey`
com a mesma semântica, mais dedup pela inbox
(`UNIQUE(consumer_name, message_id)` + hash do payload; mensagem só é
removida após commit do tratamento). Publicação repetida de evento usa a
identidade estável `(aggregate_id, event_type)` / `eventId`.

## 7. Reversões e referências pendentes

`REFUND`/`ROLLBACK` exigem `referenceExternalTransactionId`, resolvido por
`(providerId, reference)` com lock `FOR UPDATE` na referência; provedor,
jogador, carteira, moeda, rodada e valor precisam coincidir; sem devolução
duplicada do mesmo débito. Referência ausente/pendente/sem sucesso:
a reversão persiste como `PENDING_REFERENCE`, sem movimento.

Worker de resolução (`ReferenceResolver`, `internal/application`): varre
`PENDING_REFERENCE` vencidos (`pending_next_attempt_at <= now()`, índice
parcial, migration 005 com `pending_attempts`) e, por linha, numa única
transação com locks linha → wallet → referência: referência agora
PROCESSED e válida → aplica o movimento (ledger + outbox) e conclui;
referência terminal sem sucesso → REJECTED correspondente; ainda
indisponível → reagenda com backoff 2^(n-1)s (teto configurável); ao
esgotar `MaxAttempts` (default 10) → REJECTED/`REFERENCE_NOT_FOUND` com
evento de rejeição. Estado todo no banco (restart-safe, multi-instância:
`FOR UPDATE` serializa, guard de status impede duplo desfecho). Reentrega
da mesma identidade continua respondendo sem reescrever. Config via
`PENDING_RESOLVER_*` (ver `.env.example`). Ciclo de vida Fx como consumer
e publisher.

## 8. Inbox, outbox, SQS, retry e DLQ

Consumer (`wager-consumer`): long polling (20s), batch até 10, visibility
30s; parse/validação → inbox + domínio + outbox no mesmo commit →
delete da mensagem; rejeição de negócio confirmada deleta; falha
transitória deixa visibilidade expirar (retry); shutdown gracioso conclui
ou libera o trabalho. Publisher: polling de 1s, 10 linhas/ciclo, ordenado
por `next_attempt_at`; falha de `SendMessage` grava
`attempts+1`/`next_attempt_at=now()+2^(n-1)s` (teto configurável, sem
jitter) atomicamente; após `SQS_OUTBOX_MAX_ATTEMPTS` (default 5) o corpo
idêntico vai para a DLQ com `dead_lettered_at` (`published_at` continua
NULL); sem DLQ, reagenda no teto. Crash entre send e mark republica o
mesmo `eventId`. Filas locais: `wager-input`, `wager-events`,
`wager-events-dlq` (padrão, sem FIFO — Anexo do README). Limites via env
`SQS_*` (ver `.env.example`).

## 9. Autenticação e autorização

OIDC externo obrigatório e fail-closed (`OIDC_ENABLED` default true; sem
`OIDC_ISSUER` o servidor recusa iniciar). `auth.Verifier`: discovery +
JWKS, somente RS256, valida assinatura, `exp` (leeway 30s), `iss` e `aud`
(quando configurada). Convenção do projeto (o README não define o claim):
`provider_id` carrega o provedor efetivo; sem ele → `PROVIDER_UNKNOWN`.
`RequireProviderAuth` vincula `providerId` do corpo ao autenticado
(impersonação → `PROVIDER_MISMATCH`); leituras por provedor conferem o
dono sem vazar existência. `RequireInternalServiceAuth` restringe
`POST /wallets` e reconciliação ao UUID reservado
`00000000-0000-0000-0000-000000000001`. Keycloak 26 local com realm
`wager` importado de `keycloak/realm-wager.json` e 3 service accounts de
teste (`wager-provider-a/b`, `wager-internal`, segredos exclusivamente
locais documentados no compose e verificáveis via client_credentials).

## 10. Reconciliação

`POST /wallets/{walletId}/reconciliation` (interno). Lê a carteira e TODAS
as entradas (`ListAllByWallet`, sem paginação, ordem `(created_at, id)`),
parte de zero (CREDIT soma, DEBIT subtrai) e valida: primeiro
`balanceBefore == 0`, continuidade da cadeia, `balanceAfter =
balanceBefore ± amount`, amounts positivos, direções conhecidas, ausência
de saldo negativo e saldo final contra `wallet.balance`. Divergências
viram issues tipadas + `consistent: false` + `difference` (magnitude
absoluta, pois `Money` não representa negativos) + log com o flag e
métrica `wallet_reconciliations_total{result}`. Nunca altera saldo.

## 11. Observabilidade e operação local

Logs JSON (`slog`) com `request_id`/`correlationId`/`messageId`/
`transactionId`/`walletId`/`providerId`; métricas próprias de baixa
cardinalidade (requests, transações, rejeições, SQS, outbox, DLQ,
reconciliações, duração, health gauges) em formato Prometheus; health
checks live/ready (ready cobre PostgreSQL e SQS). `docker compose up`
sobe db (+`migrate` one-shot idempotente), localstack (filas via
`ready.d`), keycloak (realm importado) e `app` (imagem multi-stage
`golang:1.27-alpine` → `alpine:3.21`, usuário sem privilégio).
`docker compose up --build` reproduz o build. Testes: `go test ./...
-p=1 -count=1` (sequencial: pacotes compartilham o Postgres de teste e
limpezas globais interferem em paralelo), `go test -race ./... -p=1`,
`go vet ./...`, `gofmt -l` limpo.

## 12. Limitações, interpretações e trabalho não concluído

- `difference` da reconciliação é magnitude absoluta, não "armazenado
  menos reconstruído" com sinal (impossível em `Money`, §6.1); documentado.
- Reconciliação lê wallet e ledger em dois `SELECT`s sem transação única:
  sem snapshot pontual sob escrita concorrente.
- Sem REVOKE de UPDATE/DELETE por role: a proteção é via trigger (vale
  para qualquer role, inclusive testes — que usam TRUNCATE na limpeza).
- Testes de integração usam Postgres e LocalStack reais e IdP `httptest`;
  Keycloak real validado via smoke manual (realm importado + fluxos
  autenticados + isolation), não como teste automatizado. §13: mesma aposta
  50× em 3 instâncias (pools/services independentes), 80/80 sobre 100.00,
  carteiras distintas em paralelo, dois publishers em disputa (store real),
  redelivery/inbox, retry/DLQ, expiração de pendência e restart (rebuild de
  pool/services com replay + resolução); sem suíte multi-processo SO nem
  restart de binário automatizados.
- Filas locais são padrão (sem FIFO); sem redrive policy configurada no
  broker — DLQ da outbox é gerenciada pelo publisher.
- `ARCHITECTURE.md` substitui as notas de entrega anteriores (`handoff.md`
  é histórico de trabalho, não normativo).
