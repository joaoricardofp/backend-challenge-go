-- 005_pending_reference_retry.sql
--
-- Tentativas de resolução de PENDING_REFERENCE (README §7: worker com
-- backoff exponencial e número máximo de tentativas; ao esgotar, a
-- reversão é finalizada como REJECTED com referência não encontrada).
--
-- Duas colunas novas em wager_transactions, usadas SOMENTE por linhas
-- PENDING_REFERENCE; demais linhas mantêm os defaults, inertes:
--   pending_attempts         = resoluções já tentadas pelo worker
--   pending_next_attempt_at  = próxima tentativa devida (o poll ordena por ela)
--
-- Aplica-se sobre 004. Não edita 001-004. Aditiva e não destrutiva.
ALTER TABLE wager_transactions
    ADD COLUMN pending_attempts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_pending_attempts_non_negative
        CHECK (pending_attempts >= 0);

ALTER TABLE wager_transactions
    ADD COLUMN pending_next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX wager_transactions_pending_reference_due_idx
    ON wager_transactions (pending_next_attempt_at)
    WHERE status = 'PENDING_REFERENCE';

-- DOWN (reversão documentada; executar manualmente se necessário):
--   DROP INDEX wager_transactions_pending_reference_due_idx;
--   ALTER TABLE wager_transactions DROP COLUMN pending_next_attempt_at;
--   ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_pending_attempts_non_negative;
--   ALTER TABLE wager_transactions DROP COLUMN pending_attempts;
-- O DOWN descarta o progresso de tentativas; linhas PENDING_REFERENCE
-- permanecem pendentes, sem efeito financeiro (nunca houve movimento).
