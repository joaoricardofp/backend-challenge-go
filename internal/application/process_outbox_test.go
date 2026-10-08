package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// outboxRows lista os eventos de um agregado (ID da wager_transaction).
func outboxRows(t *testing.T, f *serviceFixture, aggregateID string) []*postgres.OutboxEvent {
	t.Helper()

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	events, err := f.outbox.ListByAggregate(ctx, dbTx, aggregateID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	return events
}

func outboxByType(t *testing.T, f *serviceFixture, aggregateID, eventType string) *postgres.OutboxEvent {
	t.Helper()

	for _, ev := range outboxRows(t, f, aggregateID) {
		if ev.EventType == eventType {
			return ev
		}
	}
	t.Fatalf("event %q not found for aggregate %q", eventType, aggregateID)
	return nil
}

func outboxPayload(t *testing.T, ev *postgres.OutboxEvent) map[string]any {
	t.Helper()

	var out map[string]any
	if err := json.Unmarshal(ev.Payload, &out); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return out
}

func assertUnpublished(t *testing.T, events []*postgres.OutboxEvent) {
	t.Helper()

	for _, ev := range events {
		if ev.PublishedAt != nil {
			t.Errorf("event %s published, want NULL (worker publica em etapa posterior)", ev.EventType)
		}
		if ev.OccurredAt.IsZero() {
			t.Errorf("event %s without occurred_at", ev.EventType)
		}
	}
}

func TestProcessWager_OutboxAtomicSuccess(t *testing.T) {
	// BEGIN: wallet + transaction + ledger + outbox (2 linhas) -> COMMIT.
	// Prova wallet, transaction, ledger e outbox existindo após o commit.
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	res, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !res.Transaction.IsProcessed() {
		t.Fatalf("status = %q, want PROCESSED", res.Transaction.Status)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 7000 || stored.Version != 2 {
		t.Errorf("wallet = %d/v%d, want 7000/v2", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}

	events := outboxRows(t, f, input.Transaction.ID)
	if len(events) != 2 {
		t.Fatalf("outbox events = %d, want 2 (decisão + saldo)", len(events))
	}
	assertUnpublished(t, events)

	decided := outboxByType(t, f, input.Transaction.ID, domain.EventWagerTransactionProcessed)
	// Identidade estável é (aggregate_id, event_type); o id da linha é um
	// UUID próprio, repetido no eventId do envelope (uma transação emite
	// vários tipos ao longo da vida, ex. PENDING_REFERENCE → PROCESSED).
	if decided.ID == "" || decided.ID == input.Transaction.ID {
		t.Errorf("decision event id = %q, want unique non-transaction id", decided.ID)
	}
	payload := outboxPayload(t, decided)
	if payload["eventId"] != decided.ID {
		t.Errorf("envelope eventId = %v, want row id %q", payload["eventId"], decided.ID)
	}
	if payload["eventType"] != domain.EventWagerTransactionProcessed || payload["aggregateId"] != input.Transaction.ID {
		t.Errorf("envelope = %v", payload)
	}
	if payload["correlationId"] != input.Transaction.IdempotencyKey {
		t.Errorf("correlationId = %v, want idempotency key", payload["correlationId"])
	}
	data := payload["data"].(map[string]any)
	if data["status"] != "PROCESSED" || data["transactionId"] != input.Transaction.ID {
		t.Errorf("data = %v", data)
	}
	if data["money"].(map[string]any)["amount"] != "30.00" {
		t.Errorf("money = %v, want decimal string", data["money"])
	}
	if data["resultingBalance"].(map[string]any)["amount"] != "70.00" {
		t.Errorf("resultingBalance = %v", data["resultingBalance"])
	}

	balance := outboxByType(t, f, input.Transaction.ID, domain.EventWalletBalanceChanged)
	bdata := outboxPayload(t, balance)["data"].(map[string]any)
	if bdata["walletId"] != wallet.ID || bdata["transactionId"] != input.Transaction.ID {
		t.Errorf("balance identity = %v", bdata)
	}
	if bdata["direction"] != "DEBIT" {
		t.Errorf("direction = %v, want DEBIT", bdata["direction"])
	}
	if bdata["money"].(map[string]any)["amount"] != "30.00" {
		t.Errorf("money = %v", bdata["money"])
	}
	if bdata["balanceBefore"].(map[string]any)["amount"] != "100.00" {
		t.Errorf("balanceBefore = %v", bdata["balanceBefore"])
	}
	if bdata["balanceAfter"].(map[string]any)["amount"] != "70.00" {
		t.Errorf("balanceAfter = %v", bdata["balanceAfter"])
	}
	if bdata["walletVersion"] != float64(2) {
		t.Errorf("walletVersion = %v, want 2", bdata["walletVersion"])
	}
}

func TestProcessWager_OutboxLossNoBalanceEvent(t *testing.T) {
	// LOSS processado produz WagerTransactionProcessed, sem WalletBalanceChanged.
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := lossInput(t, f, wallet)

	if _, err := f.service.Process(context.Background(), input); err != nil {
		t.Fatalf("Process: %v", err)
	}
	events := outboxRows(t, f, input.Transaction.ID)
	if len(events) != 1 {
		t.Fatalf("outbox events = %d, want 1 (só decisão)", len(events))
	}
	if events[0].EventType != domain.EventWagerTransactionProcessed {
		t.Errorf("type = %q, want WagerTransactionProcessed", events[0].EventType)
	}
}

func TestProcessWager_OutboxRejected(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 10001, "BRL")

	if _, err := f.service.Process(context.Background(), input); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("error = %v, want ErrInsufficientFunds", err)
	}
	events := outboxRows(t, f, input.Transaction.ID)
	if len(events) != 1 {
		t.Fatalf("outbox events = %d, want 1 (rejeição)", len(events))
	}
	ev := events[0]
	if ev.EventType != domain.EventWagerTransactionRejected {
		t.Fatalf("type = %q, want WagerTransactionRejected", ev.EventType)
	}
	data := outboxPayload(t, ev)["data"].(map[string]any)
	if data["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Errorf("failureCode = %v", data["failureCode"])
	}
	if data["resultingBalance"] == nil {
		t.Error("rejected event must carry resulting balance")
	}
	// Sem efeito financeiro e sem evento de saldo.
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_OutboxFailed(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, math.MaxInt64)
	input := f.newInput(t, wallet, domain.TransactionWin, 1, "BRL")

	if _, err := f.service.Process(context.Background(), input); !errors.Is(err, domain.ErrOverflow) {
		t.Fatalf("error = %v, want ErrOverflow", err)
	}
	events := outboxRows(t, f, input.Transaction.ID)
	if len(events) != 1 {
		t.Fatalf("outbox events = %d, want 1 (falha)", len(events))
	}
	ev := events[0]
	if ev.EventType != domain.EventWagerTransactionFailed {
		t.Fatalf("type = %q, want WagerTransactionFailed", ev.EventType)
	}
	data := outboxPayload(t, ev)["data"].(map[string]any)
	if data["failureCode"] != "OVERFLOW" {
		t.Errorf("failureCode = %v", data["failureCode"])
	}
	if _, ok := data["resultingBalance"]; ok {
		t.Error("failed event must not carry resulting balance")
	}
}

func TestProcessWager_OutboxPendingReference(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-never-existed")

	res, err := f.service.Process(context.Background(), revInput)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !res.Transaction.IsPendingReference() {
		t.Fatalf("status = %q, want PENDING_REFERENCE", res.Transaction.Status)
	}
	events := outboxRows(t, f, revInput.Transaction.ID)
	if len(events) != 1 {
		t.Fatalf("outbox events = %d, want 1 (espera)", len(events))
	}
	if events[0].EventType != domain.EventWagerTransactionPendingReference {
		t.Errorf("type = %q, want WagerTransactionPendingReference", events[0].EventType)
	}
}

func TestProcessWager_OutboxFailureRollsBackEverything(t *testing.T) {
	// Teste central da B3.11: operação financeira OK + INSERT da outbox com
	// falha -> ROLLBACK total. Nada de financeiro pode ficar committed sem
	// o evento correspondente.
	//
	// A falha é forçada por uma linha pré-existente com a mesma identidade
	// lógica (aggregate_id = tx.ID, event_type = decisão): o INSERT da
	// outbox viola a UNIQUE da 003 dentro da transação do service.
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	seedID := testUUID(t, f.pool)
	if _, err := f.pool.Exec(context.Background(),
		`INSERT INTO outbox_events (id, event_type, aggregate_id, payload, occurred_at)
		 VALUES ($1, $2, $3, $4, now())`,
		seedID, domain.EventWagerTransactionProcessed, input.Transaction.ID, `{}`,
	); err != nil {
		t.Fatalf("seed outbox conflict: %v", err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE id = $1`, seedID)
	})

	_, err := f.service.Process(context.Background(), input)
	if err == nil {
		t.Fatal("expected outbox failure, got nil")
	}
	if !errors.Is(err, postgres.ErrDuplicateOutboxEvent) {
		t.Fatalf("error = %v, want ErrDuplicateOutboxEvent (falha veio da outbox)", err)
	}

	// ROLLBACK completo: wallet intacta, sem transaction, sem ledger e sem
	// linhas novas na outbox (só a seed permanece).
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1 (revertida)", stored.Balance.Cents(), stored.Version)
	}
	f.transactionNotFound(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0 (revertido)", len(entries))
	}
	events := outboxRows(t, f, input.Transaction.ID)
	if len(events) != 1 || events[0].ID != seedID {
		t.Errorf("outbox rows = %d, want só a seed (rollback da tentativa)", len(events))
	}
}

func TestProcessWager_OutboxIdempotentNoDuplicates(t *testing.T) {
	// Reprocessar a mesma identidade retorna replay sem escrever: nenhuma
	// linha nova na outbox para a mesma transação.
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	if _, err := f.service.Process(context.Background(), input); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	first := outboxRows(t, f, input.Transaction.ID)
	if len(first) != 2 {
		t.Fatalf("outbox events = %d, want 2", len(first))
	}

	second, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("replay Process: %v", err)
	}
	if !second.Replayed {
		t.Error("not marked as replay")
	}
	again := outboxRows(t, f, input.Transaction.ID)
	if len(again) != 2 {
		t.Fatalf("outbox events after replay = %d, want still 2", len(again))
	}
	for i := range first {
		if first[i].ID != again[i].ID || first[i].EventType != again[i].EventType {
			t.Errorf("row %d changed across replay: %+v vs %+v", i, first[i], again[i])
		}
	}
}
