#!/bin/sh
# Aplica migrations/001-004 em ordem (one-shot do Compose). Idempotente via
# tabela própria schema_migrations: NÃO edita os arquivos de migration.
set -e
DB="${DATABASE_URL:?DATABASE_URL is required}"
psql "$DB" -v ON_ERROR_STOP=1 -c "CREATE TABLE IF NOT EXISTS schema_migrations (filename TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now());"
# Baseline: bancos criados antes desta tabela de controle já possuem o
# schema 001-004 se wallets existe com outbox_events.dead_lettered_at.
has_tables=$(psql "$DB" -t -c "SELECT to_regclass('public.wallets') IS NOT NULL AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'outbox_events' AND column_name = 'dead_lettered_at');")
if [ "$(echo "$has_tables" | tr -d ' ')" = "t" ]; then
  echo "baseline: schema 001-004 already present, marking applied"
  for f in /migrations/001_initial_schema.sql /migrations/002_align_transaction_schema.sql /migrations/003_outbox_event_dedup.sql /migrations/004_outbox_dead_letter.sql; do
    name=$(basename "$f")
    psql "$DB" -v ON_ERROR_STOP=1 -c "INSERT INTO schema_migrations (filename) VALUES ('$name') ON CONFLICT DO NOTHING;"
  done
fi
for f in /migrations/001_initial_schema.sql /migrations/002_align_transaction_schema.sql /migrations/003_outbox_event_dedup.sql /migrations/004_outbox_dead_letter.sql; do
  name=$(basename "$f")
  applied=$(psql "$DB" -t -c "SELECT count(*) FROM schema_migrations WHERE filename = '$name';")
  if [ "$(echo "$applied" | tr -d ' ')" = "0" ]; then
    echo "applying $name"
    psql "$DB" -v ON_ERROR_STOP=1 -f "$f"
    psql "$DB" -v ON_ERROR_STOP=1 -c "INSERT INTO schema_migrations (filename) VALUES ('$name');"
  else
    echo "skipping $name (already applied)"
  fi
done
