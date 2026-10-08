-- 006_ledger_append_only.sql
--
-- Imutabilidade do ledger no banco (README §5.8 e §6.4): correções
-- financeiras exigem novos lançamentos; UPDATE e DELETE em
-- wallet_ledger_entries são rejeitados pelo PostgreSQL, independente da
-- aplicação. INSERT permanece permitido. TRUNCATE continua possível para
-- manutenção operacional (não dispara triggers de linha).
--
-- Aplica-se sobre 005. Não edita 001-005. Aditiva e não destrutiva.
CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (operation % blocked)', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS wallet_ledger_entries_no_update_delete ON wallet_ledger_entries;

CREATE TRIGGER wallet_ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW
    EXECUTE FUNCTION prevent_ledger_mutation();

-- DOWN (reversão documentada; executar manualmente se necessário):
--   DROP TRIGGER wallet_ledger_entries_no_update_delete ON wallet_ledger_entries;
--   DROP FUNCTION prevent_ledger_mutation();
-- O DOWN volta a permitir UPDATE/DELETE; linhas existentes não são afetadas.
