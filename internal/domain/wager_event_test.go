package domain_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

func eventTestMoney(t *testing.T, cents int64) domain.Money {
	t.Helper()

	m, err := domain.NewMoney(cents, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	return m
}

func processedTx(t *testing.T) domain.WagerTransaction {
	t.Helper()

	wt, err := domain.NewWagerTransaction(
		"tx-1", "provider-1", "ext-1", "key-1", "hash-1",
		"player-1", "wallet-1", "round-1", "game-1",
		domain.TransactionBet, eventTestMoney(t, 2500), "",
	)
	if err != nil {
		t.Fatalf("new transaction: %v", err)
	}
	if err := wt.Complete(eventTestMoney(t, 7500)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	return *wt
}

func envelopeOf(t *testing.T, ev *domain.WagerEvent) map[string]any {
	t.Helper()

	raw, err := ev.Payload()
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func TestWagerEvent_Processed(t *testing.T) {
	tx := processedTx(t)
	ev, err := domain.NewWagerTransactionProcessedEvent("evt-1", tx, time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	if ev.EventType != domain.EventWagerTransactionProcessed || ev.Version != 1 {
		t.Errorf("envelope = %+v", ev)
	}
	if ev.AggregateID != "tx-1" || ev.CorrelationID != "key-1" || ev.EventID != "evt-1" {
		t.Errorf("identity = %+v", ev)
	}
	if ev.OccurredAt != "2026-10-07T12:00:00Z" {
		t.Errorf("occurredAt = %q, want RFC3339 UTC", ev.OccurredAt)
	}

	env := envelopeOf(t, ev)
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "occurredAt", "version", "data"} {
		if _, ok := env[k]; !ok {
			t.Errorf("envelope missing %q", k)
		}
	}
	if _, ok := env["causationId"]; ok {
		t.Error("causationId should be absent in this stage")
	}
	data := env["data"].(map[string]any)
	if data["status"] != "PROCESSED" || data["kind"] != "BET" {
		t.Errorf("data = %v", data)
	}
	money := data["money"].(map[string]any)
	if money["amount"] != "25.00" || money["currency"] != "BRL" {
		t.Errorf("money = %v, want decimal strings", money)
	}
	bal := data["resultingBalance"].(map[string]any)
	if bal["amount"] != "75.00" {
		t.Errorf("resultingBalance = %v", bal)
	}

	// Determinístico: mesmo evento, mesmos bytes. Sem float no payload.
	a, _ := ev.Payload()
	b, _ := ev.Payload()
	if string(a) != string(b) {
		t.Error("payload not deterministic")
	}
	if strings.Contains(string(a), "2500") && !strings.Contains(string(a), `"25.00"`) {
		t.Error("payload should carry decimal strings, not raw cents")
	}
	if !strings.Contains(string(a), `"amount":"25.00"`) {
		t.Errorf("payload missing decimal money: %s", a)
	}
}

func TestWagerEvent_Rejected(t *testing.T) {
	tx := processedTx(t)
	// Rejeitada: reconstrói como REJECTED com failure + resulting.
	wt, err := domain.NewWagerTransaction(
		"tx-2", "provider-1", "ext-2", "key-2", "hash-1",
		"player-1", "wallet-1", "round-1", "game-1",
		domain.TransactionBet, eventTestMoney(t, 2500), "",
	)
	if err != nil {
		t.Fatalf("new transaction: %v", err)
	}
	if err := wt.Reject("INSUFFICIENT_FUNDS"); err != nil {
		t.Fatalf("reject: %v", err)
	}
	bal := eventTestMoney(t, 10000)
	wt.ResultingBalance = &bal

	ev, err := domain.NewWagerTransactionRejectedEvent("evt-2", *wt, time.Now())
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	data := envelopeOf(t, ev)["data"].(map[string]any)
	if data["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Errorf("failureCode = %v", data["failureCode"])
	}
	if data["resultingBalance"] == nil {
		t.Error("rejected event must carry resulting balance")
	}
	_ = tx
}

func TestWagerEvent_Failed(t *testing.T) {
	wt, err := domain.NewWagerTransaction(
		"tx-3", "provider-1", "ext-3", "key-3", "hash-1",
		"player-1", "wallet-1", "round-1", "game-1",
		domain.TransactionWin, eventTestMoney(t, 1), "",
	)
	if err != nil {
		t.Fatalf("new transaction: %v", err)
	}
	if err := wt.Fail("OVERFLOW"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	ev, err := domain.NewWagerTransactionFailedEvent("evt-3", *wt, time.Now())
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	data := envelopeOf(t, ev)["data"].(map[string]any)
	if data["failureCode"] != "OVERFLOW" {
		t.Errorf("failureCode = %v", data["failureCode"])
	}
	if _, ok := data["resultingBalance"]; ok {
		t.Error("failed event must not carry resulting balance")
	}
}

func TestWagerEvent_PendingReference(t *testing.T) {
	wt, err := domain.NewWagerTransaction(
		"tx-4", "provider-1", "ext-4", "key-4", "hash-1",
		"player-1", "wallet-1", "round-1", "game-1",
		domain.TransactionRefund, eventTestMoney(t, 2500), "ext-1",
	)
	if err != nil {
		t.Fatalf("new transaction: %v", err)
	}
	if err := wt.MarkPendingReference(); err != nil {
		t.Fatalf("mark: %v", err)
	}
	ev, err := domain.NewWagerTransactionPendingReferenceEvent("evt-4", *wt, time.Now())
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	if ev.EventType != domain.EventWagerTransactionPendingReference {
		t.Errorf("type = %q", ev.EventType)
	}
	data := envelopeOf(t, ev)["data"].(map[string]any)
	if data["status"] != "PENDING_REFERENCE" {
		t.Errorf("status = %v", data["status"])
	}
	if data["referenceExternalTransactionId"] != "ext-1" {
		t.Errorf("reference = %v", data["referenceExternalTransactionId"])
	}
}

func TestWagerEvent_BalanceChanged(t *testing.T) {
	entry, err := domain.NewLedgerEntry(
		"ledger-1", "wallet-1", "tx-1", domain.LedgerDebit,
		eventTestMoney(t, 2500), eventTestMoney(t, 10000), eventTestMoney(t, 7500),
	)
	if err != nil {
		t.Fatalf("new ledger entry: %v", err)
	}
	ev, err := domain.NewWalletBalanceChangedEvent("ledger-1", *entry, 2, "key-1", time.Now())
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	if ev.EventType != domain.EventWalletBalanceChanged || ev.AggregateID != "tx-1" {
		t.Errorf("envelope = %+v", ev)
	}
	data := envelopeOf(t, ev)["data"].(map[string]any)
	if data["walletId"] != "wallet-1" || data["transactionId"] != "tx-1" || data["direction"] != "DEBIT" {
		t.Errorf("identity = %v", data)
	}
	if data["money"].(map[string]any)["amount"] != "25.00" {
		t.Errorf("money = %v", data["money"])
	}
	if data["balanceBefore"].(map[string]any)["amount"] != "100.00" {
		t.Errorf("balanceBefore = %v", data["balanceBefore"])
	}
	if data["balanceAfter"].(map[string]any)["amount"] != "75.00" {
		t.Errorf("balanceAfter = %v", data["balanceAfter"])
	}
	if data["walletVersion"] != float64(2) {
		t.Errorf("walletVersion = %v", data["walletVersion"])
	}
}

func TestWagerEvent_WrongStatusRejected(t *testing.T) {
	tx := processedTx(t) // PROCESSED
	if _, err := domain.NewWagerTransactionRejectedEvent("e", tx, time.Now()); err == nil {
		t.Error("rejected constructor accepted PROCESSED transaction")
	}
	if _, err := domain.NewWagerTransactionFailedEvent("e", tx, time.Now()); err == nil {
		t.Error("failed constructor accepted PROCESSED transaction")
	}
	if _, err := domain.NewWagerTransactionPendingReferenceEvent("e", tx, time.Now()); err == nil {
		t.Error("pending constructor accepted PROCESSED transaction")
	}
	pending := tx
	pending.Status = domain.TransactionPending
	if _, err := domain.NewWagerTransactionProcessedEvent("e", pending, time.Now()); err == nil {
		t.Error("processed constructor accepted PENDING transaction")
	}
	if _, err := domain.NewWagerTransactionProcessedEvent("", tx, time.Now()); err == nil {
		t.Error("empty event id accepted")
	}
}
