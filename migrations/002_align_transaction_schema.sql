-- 002_align_transaction_schema.sql
--
-- Alinha o PostgreSQL ao modelo oficial (README/spec) já implementado no
-- domínio (etapas A1-A4.1):
--   - amount >= 0 (LOSS e OPENING com 0.00 passam a ser persistíveis);
--   - kinds oficiais: OPENING, BET, WIN, LOSS, REFUND, ROLLBACK;
--   - status oficiais: PENDING, PENDING_REFERENCE, PROCESSED, REJECTED, FAILED;
--   - reference_external_transaction_id obrigatória para REFUND/ROLLBACK.
--
-- Aplica-se sobre 001_initial_schema.sql. Não edita a 001.
-- Falha em vez de modificar dados: os ADD CONSTRAINT validam as linhas
-- existentes e abortam se houver kind/status/amount legado incompatível.
--
-- Decisões deliberadas (ver relatório B1):
--   - provider_id, external_transaction_id, idempotency_key e payload_hash
--     permanecem NOT NULL para todos os kinds: o construtor de domínio
--     (A3.1) os exige inclusive para OPENING, e a identidade interna do
--     OPENING ainda será definida em etapa própria. round_id, game_id e
--     reference_external_transaction_id já são nullable na 001.
--   - a matriz amount-por-kind (BET/WIN/REFUND/ROLLBACK > 0, LOSS = 0)
--     permanece no domínio; o schema garante apenas amount >= 0.
--   - sem FK nova para a referência: a resolução é da aplicação.
--   - ledger e wallets não são tocados.

-- 1. Amount: permitir zero (antes: amount > 0).
ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_amount_positive;

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_amount_non_negative
        CHECK (amount >= 0);

-- 2. Kinds oficiais. Remove DEBIT/CREDIT como transaction kind
-- (continuam válidos como direção em wallet_ledger_entries).
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_kind_allowed
        CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'));

-- 3. Status oficiais. Remove COMPLETED.
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_status_allowed
        CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED'));

-- 4. Referência obrigatória (não vazia) para reversões.
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_reversal_reference_required
        CHECK (
            (kind IN ('REFUND', 'ROLLBACK')
                AND reference_external_transaction_id IS NOT NULL
                AND reference_external_transaction_id <> '')
            OR kind NOT IN ('REFUND', 'ROLLBACK')
        );

-- DOWN (reversão documentada; executar manualmente se necessário):
--   ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_reversal_reference_required;
--   ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_status_allowed;
--   ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_kind_allowed;
--   ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_amount_non_negative;
--   ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_amount_positive CHECK (amount > 0);
-- O DOWN falha se existirem linhas com amount = 0 ou kind/status novos.
