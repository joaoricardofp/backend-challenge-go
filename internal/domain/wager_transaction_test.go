package domain

import (
	"errors"
	"testing"
)

func validTransaction(t *testing.T) *WagerTransaction {
	t.Helper()

	amount := mustMoney(t, 5000, "BRL")

	tx, err := NewWagerTransaction(
		"tx-1",
		"provider-1",
		"ext-tx-1",
		"idem-key-1",
		"hash-abc",
		"player-1",
		"wallet-1",
		"round-1",
		"game-1",
		TransactionDebit,
		amount,
	)
	if err != nil {
		t.Fatalf("NewWagerTransaction: unexpected error: %v", err)
	}

	return tx
}

func TestNewWagerTransaction(t *testing.T) {
	amount := mustMoney(t, 5000, "BRL")
	zeroAmount := mustMoney(t, 0, "BRL")

	tests := []struct {
		name                  string
		id                    string
		providerID            string
		externalTransactionID string
		idempotencyKey        string
		payloadHash           string
		playerID              string
		walletID              string
		roundID               string
		gameID                string
		kind                  WagerTransactionKind
		amount                Money
		wantErr               bool
		wantErrMsg            string
	}{
		{
			name:                  "valid debit",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
		},
		{
			name:                  "valid credit",
			id:                    "tx-2",
			providerID:            "prov-1",
			externalTransactionID: "ext-2",
			idempotencyKey:        "key-2",
			payloadHash:           "hash-2",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionCredit,
			amount:                amount,
		},
		{
			name:                  "empty id",
			id:                    "",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "transaction id is required",
		},
		{
			name:                  "empty provider id",
			id:                    "tx-1",
			providerID:            "",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "provider id is required",
		},
		{
			name:                  "empty external transaction id",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "external transaction id is required",
		},
		{
			name:                  "empty idempotency key",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "idempotency key is required",
		},
		{
			name:                  "empty payload hash",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "payload hash is required",
		},
		{
			name:                  "empty player id",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "player id is required",
		},
		{
			name:                  "empty wallet id",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "wallet id is required",
		},
		{
			name:                  "empty round id",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "round id is required",
		},
		{
			name:                  "empty game id",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "",
			kind:                  TransactionDebit,
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            "game id is required",
		},
		{
			name:                  "invalid kind",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  "REFUND",
			amount:                amount,
			wantErr:               true,
			wantErrMsg:            ErrInvalidTransactionKind.Error(),
		},
		{
			name:                  "zero amount",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionDebit,
			amount:                zeroAmount,
			wantErr:               true,
			wantErrMsg:            ErrInvalidAmount.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewWagerTransaction(
				tt.id, tt.providerID, tt.externalTransactionID,
				tt.idempotencyKey, tt.payloadHash,
				tt.playerID, tt.walletID,
				tt.roundID, tt.gameID,
				tt.kind, tt.amount,
			)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error %q, got nil", tt.wantErrMsg)
				}
				if err.Error() != tt.wantErrMsg {
					t.Errorf("error = %q, want %q", err.Error(), tt.wantErrMsg)
				}
				if got != nil {
					t.Errorf("expected nil transaction on error, got %+v", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.ID != tt.id {
				t.Errorf("ID = %q, want %q", got.ID, tt.id)
			}
			if got.Status != TransactionPending {
				t.Errorf("Status = %q, want %q", got.Status, TransactionPending)
			}
			if got.Kind != tt.kind {
				t.Errorf("Kind = %q, want %q", got.Kind, tt.kind)
			}
			if got.ResultingBalance != nil {
				t.Errorf("ResultingBalance = %+v, want nil", got.ResultingBalance)
			}
			if got.FailureCode != "" {
				t.Errorf("FailureCode = %q, want empty", got.FailureCode)
			}
		})
	}
}

func TestWagerTransaction_Complete(t *testing.T) {
	t.Run("marks pending as completed", func(t *testing.T) {
		tx := validTransaction(t)

		balance := mustMoney(t, 10000, "BRL")
		if err := tx.Complete(balance); err != nil {
			t.Fatalf("Complete: unexpected error: %v", err)
		}

		if !tx.IsCompleted() {
			t.Errorf("Status = %q, want COMPLETED", tx.Status)
		}
		if tx.ResultingBalance == nil {
			t.Fatal("ResultingBalance is nil after Complete")
		}
		if tx.ResultingBalance.Cents() != 10000 {
			t.Errorf("ResultingBalance = %d, want 10000", tx.ResultingBalance.Cents())
		}
	})

	t.Run("rejects complete on completed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		err := tx.Complete(mustMoney(t, 2000, "BRL"))
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects complete on failed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Fail("INSUFFICIENT_FUNDS")

		err := tx.Complete(mustMoney(t, 2000, "BRL"))
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})
}

func TestWagerTransaction_Fail(t *testing.T) {
	t.Run("marks pending as failed", func(t *testing.T) {
		tx := validTransaction(t)

		if err := tx.Fail("INSUFFICIENT_FUNDS"); err != nil {
			t.Fatalf("Fail: unexpected error: %v", err)
		}

		if !tx.IsFailed() {
			t.Errorf("Status = %q, want FAILED", tx.Status)
		}
		if tx.FailureCode != "INSUFFICIENT_FUNDS" {
			t.Errorf("FailureCode = %q, want INSUFFICIENT_FUNDS", tx.FailureCode)
		}
	})

	t.Run("rejects fail on completed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		err := tx.Fail("SOME_ERROR")
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects fail on already failed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Fail("FIRST_ERROR")

		err := tx.Fail("SECOND_ERROR")
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects empty failure code", func(t *testing.T) {
		tx := validTransaction(t)

		err := tx.Fail("")
		if err == nil {
			t.Fatal("expected error for empty failure code")
		}

		// Status should remain pending
		if !tx.IsPending() {
			t.Errorf("Status = %q, want PENDING", tx.Status)
		}
	})
}

func TestWagerTransaction_StatusPredicates(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		tx := validTransaction(t)

		if !tx.IsPending() {
			t.Error("IsPending() = false, want true")
		}
		if tx.IsCompleted() {
			t.Error("IsCompleted() = true, want false")
		}
		if tx.IsFailed() {
			t.Error("IsFailed() = true, want false")
		}
	})

	t.Run("completed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		if tx.IsPending() {
			t.Error("IsPending() = true, want false")
		}
		if !tx.IsCompleted() {
			t.Error("IsCompleted() = false, want true")
		}
		if tx.IsFailed() {
			t.Error("IsFailed() = true, want false")
		}
	})

	t.Run("failed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Fail("SOME_ERROR")

		if tx.IsPending() {
			t.Error("IsPending() = true, want false")
		}
		if tx.IsCompleted() {
			t.Error("IsCompleted() = true, want false")
		}
		if !tx.IsFailed() {
			t.Error("IsFailed() = false, want true")
		}
	})
}
