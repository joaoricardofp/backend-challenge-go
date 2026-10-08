-- 004_outbox_dead_letter.sql
--
-- Estado dead-letter da transactional outbox (etapa B3.13).
--
-- A tabela outbox_events (001) é reutilizada como está, com uma única
-- coluna nova: dead_lettered_at. Nenhuma alteração em 001/002/003.
-- attempts/next_attempt_at (001) passam a ser usados nesta etapa para
-- retry com backoff exponencial (sem jitter); published_at continua sendo
-- preenchido somente após SendMessage com sucesso na fila de eventos.
--
-- Semântica dos estados (colunas nullable):
--   pendente     = published_at IS NULL AND dead_lettered_at IS NULL
--                  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
--   publicado    = published_at IS NOT NULL (entregue na fila de eventos)
--   dead-letter  = dead_lettered_at IS NOT NULL (tentativas esgotadas e
--                  corpo idêntico encaminhado à DLQ de eventos)
--
-- Um evento NUNCA recebe published_at por ter ido para a DLQ: publicado com
-- sucesso continua distinguível de dead-letter. O corpo enviado à DLQ é
-- byte a byte idêntico ao payload (identidade estável preservada).
--
-- Aplica-se sobre 003_outbox_event_dedup.sql.
-- Procedimento: executar os arquivos em ordem numérica no banco
-- (psql -f migrations/001_initial_schema.sql, depois 002, 003 e 004).

ALTER TABLE outbox_events
    ADD COLUMN dead_lettered_at TIMESTAMPTZ;

-- DOWN (reversão documentada; executar manualmente se necessário):
--   ALTER TABLE outbox_events DROP COLUMN dead_lettered_at;
-- O DOWN descarta a informação de dead-letter; linhas publicadas e
-- pendentes não são afetadas.
