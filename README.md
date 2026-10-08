# Jungle Gaming Backend Challenge — Go

Backend para processamento distribuído de operações financeiras de provedores de jogos, implementado em Go com Uber Fx, PostgreSQL, AWS SQS/LocalStack e Keycloak/OIDC.

A solução foi construída para preservar as garantias financeiras sob concorrência, reentrega de mensagens, reinicialização de processos e falhas entre commit e publicação.

## Stack

- Go 1.27
- Uber Fx
- PostgreSQL
- pgx com SQL explícito
- AWS SQS
- LocalStack
- Keycloak 26
- Docker Compose
- Prometheus-compatible metrics
- `testing` / `go test` / `go test -race`

## Arquitetura

A aplicação possui as seguintes fronteiras principais:

```text
HTTP
  │
  ├── OIDC / Keycloak
  │
  ▼
Application Services
  │
  ├── PostgreSQL
  │     ├── wallets
  │     ├── wager_transactions
  │     ├── wallet_ledger_entries
  │     ├── inbox
  │     └── outbox_events
  │
  └── SQS
        ├── wager-input
        ├── wager-events
        └── wager-events-dlq
```

O mesmo caso de uso financeiro é utilizado pelas entradas HTTP e SQS.

As garantias de idempotência e concorrência são persistentes no PostgreSQL e não dependem de estado em memória de uma instância.

A arquitetura detalhada, decisões técnicas e limitações conhecidas estão em [`ARCHITECTURE.md`](./ARCHITECTURE.md).

A especificação original do desafio está preservada em [`CHALLENGE.md`](./CHALLENGE.md).

---

# Reprodução local

## Pré-requisitos

Para executar o ambiente completo, instale:

- Docker
- Docker Compose
- Git

Para executar os testes Go diretamente no host:

- Go 1.27 ou compatível com a versão declarada em `go.mod`

Não é necessário instalar PostgreSQL, LocalStack ou Keycloak no host quando o ambiente Docker Compose for utilizado.

## Configuração

O repositório possui `.env.example` com valores apropriados para desenvolvimento local.

Copie-o para `.env`:

```bash
cp .env.example .env
```

Os valores locais do ambiente não devem ser substituídos por credenciais reais.

O Keycloak também possui um realm versionado em:

```text
keycloak/realm-wager.json
```

O realm é importado automaticamente pelo Compose.

---

# Subindo o ambiente completo

A forma recomendada de reproduzir a aplicação a partir de um checkout limpo é:

```bash
docker compose up --build
```

O Compose inicializa:

1. PostgreSQL;
2. migrations;
3. LocalStack;
4. filas SQS;
5. Keycloak;
6. realm `wager`;
7. aplicação.

As migrations são aplicadas de forma idempotente por meio da tabela `schema_migrations`.

Para encerrar:

```bash
docker compose down
```

Para remover também os volumes persistentes:

```bash
docker compose down -v
```

Isso é útil para reproduzir um banco completamente limpo.

---

# Banco de dados e migrations

As migrations são versionadas em `migrations/` e são aplicadas em ordem.

Atualmente o schema possui as migrations:

```text
001
002
003
004
005
006
```

Entre outras garantias, elas estabelecem:

- unicidade de carteiras;
- saldo não negativo;
- unicidade de transações externas;
- idempotência;
- referências de reversão;
- constraints do ledger;
- estado persistente de retry;
- proteção append-only do ledger.

O serviço `migrate` do Docker Compose executa as migrations antes da aplicação.

O script também consegue reconhecer um banco previamente existente e registrar o baseline sem modificar retroativamente as migrations anteriores.

As instruções de reversão ficam documentadas nos próprios arquivos de migration quando aplicável.

---

# Health checks

Depois que o ambiente estiver iniciado:

```http
GET /health/live
GET /health/ready
```

`/health/live` verifica a vida do processo.

`/health/ready` verifica as dependências necessárias, incluindo PostgreSQL e SQS.

---

# Autenticação

A API utiliza OAuth 2.0/OIDC com Keycloak.

O ambiente local possui três clients de service account:

```text
wager-provider-a
wager-provider-b
wager-internal
```

Os dois primeiros representam provedores externos. O último é reservado para operações internas.

O token deve conter:

- issuer correto;
- assinatura RS256 válida;
- `exp`;
- audience `wager-api`;
- `provider_id`.

O servidor valida o `provider_id` contra o provedor da operação e impede acesso cruzado entre providers.

## Obtendo um token local

Com o ambiente iniciado, é possível utilizar `client_credentials` contra o Keycloak local (publicado em `http://localhost:8081`).

Exemplo conceitual:

```bash
curl -X POST \
  http://localhost:8081/realms/wager/protocol/openid-connect/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials' \
  -d 'client_id=wager-provider-a' \
  -d 'client_secret=local-test-secret-provider-a'
```

O token retornado deve ser enviado como:

```http
Authorization: Bearer <token>
```

Os secrets acima são exclusivamente valores de teste do ambiente local. Não devem ser reutilizados em produção.

---

# API

A API escuta em `http://localhost:8080` quando o ambiente é iniciado via Compose.

## Abrir carteira

A abertura é uma operação interna.

```http
POST /wallets
Content-Type: application/json
Authorization: Bearer <internal-token>
```

Exemplo:

```json
{
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "initialBalance": {
    "amount": "100.00",
    "currency": "BRL"
  }
}
```

Saldo inicial positivo cria:

- carteira;
- transação `OPENING`;
- lançamento no ledger;
- eventos de outbox.

Tudo no mesmo commit.

Saldo inicial zero não cria `OPENING`, ledger ou eventos financeiros.

---

## Consultar carteira

```http
GET /wallets/{walletId}
Authorization: Bearer <token>
```

---

## Consultar ledger

```http
GET /wallets/{walletId}/ledger
Authorization: Bearer <token>
```

A paginação utiliza cursor opaco e ordenação estável por:

```text
(created_at, id)
```

---

## Processar aposta

```http
POST /wagering/transactions
Authorization: Bearer <provider-token>
Content-Type: application/json
Idempotency-Key: provider-a:transaction-123
```

Exemplo:

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": {
    "amount": "25.00",
    "currency": "BRL"
  }
}
```

O header `Idempotency-Key` é obrigatório.

O servidor nunca substitui silenciosamente a chave recebida.

Replays retornam o resultado financeiro persistido do processamento original.

---

# Operações suportadas

| Tipo | Comportamento |
|---|---|
| `BET` | Débito |
| `WIN` | Crédito |
| `LOSS` | Sem movimentação |
| `REFUND` | Crédito correspondente a uma `BET` processada |
| `ROLLBACK` | Movimento contrário à operação referenciada |

`REFUND` e `ROLLBACK` exigem:

```json
{
  "referenceExternalTransactionId": "..."
}
```

A referência é resolvida por `(providerId, referenceExternalTransactionId)`.

Se a referência ainda não existir, a operação pode permanecer em:

```text
PENDING_REFERENCE
```

O worker de resolução retoma essas operações com retry persistente, backoff exponencial e limite de tentativas.

Após esgotar as tentativas, a operação é finalizada como `REJECTED` com código `REFERENCE_NOT_FOUND`.

---

# Idempotência

A idempotência é persistida no PostgreSQL.

São consideradas as identidades:

```text
(providerId, externalTransactionId)
(providerId, idempotencyKey)
```

Uma chave reutilizada com payload diferente gera conflito.

Uma operação já concluída retorna o resultado persistido originalmente e não reaplica o movimento financeiro.

A mesma garantia é utilizada na entrada por SQS.

---

# Ledger

O ledger é append-only.

Cada lançamento registra:

- `id`;
- `walletId`;
- `transactionId`;
- direção;
- valor;
- `balanceBefore`;
- `balanceAfter`;
- `createdAt`.

O banco impede:

```text
UPDATE wallet_ledger_entries
DELETE FROM wallet_ledger_entries
```

por meio de trigger.

Correções financeiras devem ser representadas por novos lançamentos.

---

# SQS

O ambiente local cria:

```text
wager-input
wager-events
wager-events-dlq
```

Topologia:

```text
wager-input
    │
    ▼
 consumer
    │
    ▼
 inbox + WagerService
    │
    ▼
 PostgreSQL
    │
    ▼
 outbox_events
    │
    ▼
 publisher
    │
    ├──> wager-events
    │
    └──> após esgotar retries
             │
             ▼
        wager-events-dlq
```

O processamento de entrada utiliza inbox + transação PostgreSQL.

A mensagem só é removida da fila depois da confirmação do processamento durável.

Falhas transitórias permitem reentrega.

O publisher utiliza retry persistente com backoff.

O `eventId` permanece estável durante republicações.

---

# Reconciliation

Endpoint:

```http
POST /wallets/{walletId}/reconciliation
Authorization: Bearer <internal-token>
```

A reconciliação:

1. lê a carteira;
2. lê todas as entradas do ledger;
3. reconstrói o saldo;
4. valida a cadeia `balanceBefore`/`balanceAfter`;
5. valida direções e valores;
6. compara o saldo calculado com o saldo armazenado;
7. retorna as divergências encontradas.

A operação é somente leitura e nunca altera a carteira.

Exemplo:

```json
{
  "walletId": "...",
  "storedBalance": {
    "amount": "75.00",
    "currency": "BRL"
  },
  "calculatedBalance": {
    "amount": "75.00",
    "currency": "BRL"
  },
  "difference": {
    "amount": "0.00",
    "currency": "BRL"
  },
  "consistent": true,
  "checkedEntries": 3,
  "issues": []
}
```

Divergências também são registradas em logs e na métrica:

```text
wallet_reconciliations_total{result="consistent|inconsistent"}
```

---

# Testes

## Suite completa

A suíte utiliza PostgreSQL e LocalStack reais em vários testes de integração.

Como os pacotes compartilham o PostgreSQL de teste e possuem limpezas globais, a execução recomendada é sequencial:

```bash
go test ./... -p=1 -count=1
```

## Race detector

```bash
go test -race ./... -p=1 -count=1
```

## Vet

```bash
go vet ./...
```

## Formatação

```bash
gofmt -l .
```

O comando deve não produzir arquivos.

---

# Cenários de concorrência e recuperação

A suíte cobre, entre outros:

- mesma aposta recebida 50 vezes em paralelo;
- disputa entre apostas concorrentes;
- 80.00 + 80.00 sobre saldo de 100.00;
- processamento simultâneo de carteiras diferentes;
- múltiplas instâncias/pools independentes;
- reentrega de mensagens;
- inbox;
- dois publishers disputando a outbox;
- retry e DLQ;
- crash entre publicação e confirmação da outbox;
- `REFUND`/`ROLLBACK` antes da referência;
- resolução posterior de `PENDING_REFERENCE`;
- expiração de referências pendentes;
- reinicialização e replay;
- isolamento entre providers;
- fluxos OIDC com IdP de teste (Keycloak real validado por smoke manual).

Os testes de restart realizados no ambiente de validação recriam pools/serviços e verificam a recuperação persistente. A suíte automatizada não pretende simular um restart real de processo/binário do sistema operacional.

---

# Execução sem Docker

É possível executar os testes Go diretamente no host, desde que PostgreSQL e LocalStack estejam disponíveis e configurados conforme `.env`.

Para execução da aplicação sem Compose:

```bash
go run ./cmd/server
```

Nesse modo, as dependências externas precisam estar previamente disponíveis.

Para uma reprodução completa e mais simples, prefira:

```bash
docker compose up --build
```

---

# Desenvolvimento

Build:

```bash
go build ./...
```

Testes:

```bash
go test ./... -p=1 -count=1
```

Race detector:

```bash
go test -race ./... -p=1 -count=1
```

Vet:

```bash
go vet ./...
```

Formatação:

```bash
gofmt -w .
```

---

# Estrutura do projeto

```text
cmd/server
internal/
  app/
  application/
  auth/
  config/
  domain/
  messaging/
  observability/
  repository/
  transport/
migrations/
keycloak/
localstack/
Dockerfile
docker-compose.yaml
migrate.sh
.env.example
ARCHITECTURE.md
```

O domínio não depende de HTTP, SQS, Fx ou pgx.

A composição da aplicação é realizada por Uber Fx.

---

# Observabilidade

A aplicação produz logs estruturados em JSON com identificadores de correlação quando disponíveis, incluindo:

- `request_id`;
- `correlationId`;
- `messageId`;
- `transactionId`;
- `walletId`;
- `providerId`.

As métricas possuem baixa cardinalidade e cobrem resultados de requisições, transações, rejeições, SQS, outbox, DLQ, reconciliação, duração e health checks.

---

# Limitações conhecidas

As seguintes decisões são conscientes e estão detalhadas em `ARCHITECTURE.md`:

- a reconciliação executa dois `SELECT`s sem um snapshot transacional único;
- `difference` é representada como magnitude absoluta;
- a proteção append-only do ledger é feita por trigger, sem REVOKE específico de UPDATE/DELETE;
- os testes automatizados não executam restart real de processo/binário do sistema operacional;
- a validação do Keycloak real foi feita por smoke/integration manual, enquanto os testes automatizados utilizam um IdP de teste;
- as filas locais utilizam SQS padrão, sem FIFO;
- o DLQ da outbox é gerenciado pelo publisher.

Essas limitações não alteram as garantias financeiras e de idempotência implementadas para o escopo do desafio.

---

# Reprodução rápida para avaliação

Em um checkout limpo:

```bash
git clone <URL_DO_REPOSITORIO>
cd <DIRETORIO_DO_REPOSITORIO>

cp .env.example .env

docker compose up --build
```

Em outro terminal:

```bash
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
```

Executar a suíte:

```bash
go test ./... -p=1 -count=1
```

Race detector:

```bash
go test -race ./... -p=1 -count=1
```

Validação estática:

```bash
go vet ./...
```

Build:

```bash
go build ./...
```

Encerrar:

```bash
docker compose down
```

Para uma execução completamente limpa:

```bash
docker compose down -v
docker compose up --build
```

Consulte [`ARCHITECTURE.md`](./ARCHITECTURE.md) para as decisões de implementação, garantias, limites e detalhes de operação.
