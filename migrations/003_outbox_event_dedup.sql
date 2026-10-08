-- 003_outbox_event_dedup.sql
--
-- Dedup lógica da transactional outbox (etapa B3.11).
--
-- A tabela outbox_events (001) é reutilizada como está: nenhuma coluna nova,
-- nenhuma alteração em 001/002. attempts/next_attempt_at continuam existindo
-- com defaults, mas NÃO são usados nesta etapa (sem publisher, sem retry,
-- sem backoff, sem DLQ — tudo isso pertence a etapas posteriores).
--
-- A identidade estável de um evento é o par (aggregate_id, event_type),
-- onde aggregate_id é o ID da wager_transaction que originou o evento e
-- event_type é um dos tipos do contrato (WagerTransactionProcessed,
-- WagerTransactionRejected, WagerTransactionFailed,
-- WagerTransactionPendingReference, WalletBalanceChanged).
--
-- O índice único abaixo impede dois eventos do mesmo tipo para a mesma
-- transação, inclusive sob redelivery concorrente. Uma transação pode ter
-- no máximo uma linha de decisão + uma de WalletBalanceChanged (tipos
-- distintos), o que também deixa espaço para uma futura resolução de
-- PENDING_REFERENCE emitir um novo tipo sem conflito.
--
-- Aplica-se sobre 001_initial_schema.sql (+ 002, que não toca na outbox).
-- Procedimento: executar os arquivos em ordem numérica no banco
-- (psql -f migrations/001_initial_schema.sql, depois 002, depois 003).
-- O índice é idempotente apenas se ainda não existir; em caso de reexecução
-- acidental, o PostgreSQL retorna erro de objeto duplicado sem alterar dados.

CREATE UNIQUE INDEX outbox_events_aggregate_event_unique
    ON outbox_events (aggregate_id, event_type);

-- DOWN (reversão documentada; executar manualmente se necessário):
--   DROP INDEX outbox_events_aggregate_event_unique;
-- O DOWN falha se ... (não há condição de falha além de o índice não existir;
-- a remoção nunca invalida linhas, apenas volta a permitir duplicatas).
