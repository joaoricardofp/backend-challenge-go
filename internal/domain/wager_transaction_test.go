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
		TransactionBet,
		amount,
		"",
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
		reference             string
		wantErr               bool
		wantErrMsg            string
	}{
		{
			name:                  "valid bet",
			id:                    "tx-1",
			providerID:            "prov-1",
			externalTransactionID: "ext-1",
			idempotencyKey:        "key-1",
			payloadHash:           "hash-1",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionBet,
			amount:                amount,
		},
		{
			name:                  "valid win",
			id:                    "tx-2",
			providerID:            "prov-1",
			externalTransactionID: "ext-2",
			idempotencyKey:        "key-2",
			payloadHash:           "hash-2",
			playerID:              "player-1",
			walletID:              "wallet-1",
			roundID:               "round-1",
			gameID:                "game-1",
			kind:                  TransactionWin,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  TransactionBet,
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
			kind:                  "DEBIT",
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
			kind:                  TransactionBet,
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
				tt.kind, tt.amount, tt.reference,
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
			if got.ReferenceExternalTransactionID != tt.reference {
				t.Errorf("ReferenceExternalTransactionID = %q, want %q", got.ReferenceExternalTransactionID, tt.reference)
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

func TestWagerTransactionKinds(t *testing.T) {
	positive := mustMoney(t, 5000, "BRL")
	zero := mustMoney(t, 0, "BRL")

	kinds := []struct {
		kind      WagerTransactionKind
		amount    Money
		reference string
	}{
		{kind: TransactionOpening, amount: zero},
		{kind: TransactionOpening, amount: positive},
		{kind: TransactionBet, amount: positive},
		{kind: TransactionWin, amount: positive},
		{kind: TransactionWin, amount: positive, reference: "ext-bet-1"},
		{kind: TransactionLoss, amount: zero},
		{kind: TransactionRefund, amount: positive, reference: "ext-bet-1"},
		{kind: TransactionRollback, amount: positive, reference: "ext-bet-1"},
	}

	for _, k := range kinds {
		name := string(k.kind)
		if k.reference != "" {
			name += " with reference"
		}
		if k.amount.IsZero() {
			name += " zero"
		}
		t.Run(name, func(t *testing.T) {
			tx, err := NewWagerTransaction(
				"tx-1", "prov-1", "ext-1",
				"key-1", "hash-1",
				"player-1", "wallet-1",
				"round-1", "game-1",
				k.kind, k.amount, k.reference,
			)
			if err != nil {
				t.Fatalf("NewWagerTransaction(%q): unexpected error: %v", k.kind, err)
			}
			if tx.Kind != k.kind {
				t.Errorf("Kind = %q, want %q", tx.Kind, k.kind)
			}
			if tx.ReferenceExternalTransactionID != k.reference {
				t.Errorf("ReferenceExternalTransactionID = %q, want %q", tx.ReferenceExternalTransactionID, k.reference)
			}
			if tx.Status != TransactionPending {
				t.Errorf("Status = %q, want %q", tx.Status, TransactionPending)
			}
		})
	}

	t.Run("old debit kind is rejected", func(t *testing.T) {
		_, err := NewWagerTransaction(
			"tx-1", "prov-1", "ext-1",
			"key-1", "hash-1",
			"player-1", "wallet-1",
			"round-1", "game-1",
			"DEBIT", mustMoney(t, 5000, "BRL"),
			"",
		)
		if !errors.Is(err, ErrInvalidTransactionKind) {
			t.Fatalf("expected ErrInvalidTransactionKind, got %v", err)
		}
	})

	t.Run("old credit kind is rejected", func(t *testing.T) {
		_, err := NewWagerTransaction(
			"tx-1", "prov-1", "ext-1",
			"key-1", "hash-1",
			"player-1", "wallet-1",
			"round-1", "game-1",
			"CREDIT", mustMoney(t, 5000, "BRL"),
			"",
		)
		if !errors.Is(err, ErrInvalidTransactionKind) {
			t.Fatalf("expected ErrInvalidTransactionKind, got %v", err)
		}
	})
}

func TestWagerTransaction_KindAmountAndReference(t *testing.T) {
	positive := mustMoney(t, 5000, "BRL")
	zero := mustMoney(t, 0, "BRL")

	newTx := func(kind WagerTransactionKind, amount Money, roundID, gameID, reference string) (*WagerTransaction, error) {
		return NewWagerTransaction(
			"tx-1", "prov-1", "ext-1",
			"key-1", "hash-1",
			"player-1", "wallet-1",
			roundID, gameID,
			kind, amount, reference,
		)
	}

	t.Run("bet zero amount is rejected", func(t *testing.T) {
		_, err := newTx(TransactionBet, zero, "round-1", "game-1", "")
		if !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("expected ErrInvalidAmount, got %v", err)
		}
	})

	t.Run("win zero amount is rejected", func(t *testing.T) {
		_, err := newTx(TransactionWin, zero, "round-1", "game-1", "")
		if !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("expected ErrInvalidAmount, got %v", err)
		}
	})

	t.Run("loss positive amount is rejected", func(t *testing.T) {
		_, err := newTx(TransactionLoss, positive, "round-1", "game-1", "")
		if !errors.Is(err, ErrInvalidLossAmount) {
			t.Fatalf("expected ErrInvalidLossAmount, got %v", err)
		}
	})

	t.Run("refund without reference is rejected", func(t *testing.T) {
		_, err := newTx(TransactionRefund, positive, "round-1", "game-1", "")
		if err == nil {
			t.Fatal("expected error for missing reference")
		}
	})

	t.Run("refund zero amount is rejected", func(t *testing.T) {
		_, err := newTx(TransactionRefund, zero, "round-1", "game-1", "ext-bet-1")
		if !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("expected ErrInvalidAmount, got %v", err)
		}
	})

	t.Run("rollback without reference is rejected", func(t *testing.T) {
		_, err := newTx(TransactionRollback, positive, "round-1", "game-1", "")
		if err == nil {
			t.Fatal("expected error for missing reference")
		}
	})

	t.Run("rollback zero amount is rejected", func(t *testing.T) {
		_, err := newTx(TransactionRollback, zero, "round-1", "game-1", "ext-bet-1")
		if !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("expected ErrInvalidAmount, got %v", err)
		}
	})

	t.Run("opening without round and game succeeds", func(t *testing.T) {
		tx, err := newTx(TransactionOpening, positive, "", "", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tx.Kind != TransactionOpening {
			t.Errorf("Kind = %q, want OPENING", tx.Kind)
		}
	})

	t.Run("external kinds without round are rejected", func(t *testing.T) {
		for _, kind := range []WagerTransactionKind{
			TransactionBet, TransactionWin, TransactionLoss,
			TransactionRefund, TransactionRollback,
		} {
			amount := positive
			if kind == TransactionLoss {
				amount = zero
			}
			reference := ""
			if kind == TransactionRefund || kind == TransactionRollback {
				reference = "ext-bet-1"
			}
			if _, err := newTx(kind, amount, "", "game-1", reference); err == nil {
				t.Errorf("kind %q: expected error for missing round id", kind)
			}
		}
	})

	t.Run("external kinds without game are rejected", func(t *testing.T) {
		for _, kind := range []WagerTransactionKind{
			TransactionBet, TransactionWin, TransactionLoss,
			TransactionRefund, TransactionRollback,
		} {
			amount := positive
			if kind == TransactionLoss {
				amount = zero
			}
			reference := ""
			if kind == TransactionRefund || kind == TransactionRollback {
				reference = "ext-bet-1"
			}
			if _, err := newTx(kind, amount, "round-1", "", reference); err == nil {
				t.Errorf("kind %q: expected error for missing game id", kind)
			}
		}
	})
}

func reversalFixture(t *testing.T, idSuffix string, kind WagerTransactionKind, status WagerTransactionStatus) *WagerTransaction {
	t.Helper()

	amount := mustMoney(t, 5000, "BRL")
	if kind == TransactionLoss {
		amount = mustMoney(t, 0, "BRL")
	}

	tx, err := NewWagerTransaction(
		"tx-"+idSuffix, "prov-1", "ext-"+idSuffix,
		"key-"+idSuffix, "hash-1",
		"player-1", "wallet-1",
		"round-1", "game-1",
		kind, amount, "ext-ref-1",
	)
	if err != nil {
		t.Fatalf("NewWagerTransaction: unexpected error: %v", err)
	}

	switch status {
	case TransactionPending:
	case TransactionPendingReference:
		if err := tx.MarkPendingReference(); err != nil {
			t.Fatalf("MarkPendingReference: unexpected error: %v", err)
		}
	case TransactionProcessed:
		if err := tx.Complete(mustMoney(t, 7000, "BRL")); err != nil {
			t.Fatalf("Complete: unexpected error: %v", err)
		}
	case TransactionRejected:
		if err := tx.Reject("SOME_REJECTION"); err != nil {
			t.Fatalf("Reject: unexpected error: %v", err)
		}
	case TransactionFailed:
		if err := tx.Fail("INFRA_TIMEOUT"); err != nil {
			t.Fatalf("Fail: unexpected error: %v", err)
		}
	}

	return tx
}

func TestValidateReversal(t *testing.T) {
	tests := []struct {
		name            string
		referenceKind   WagerTransactionKind
		referenceStatus WagerTransactionStatus
		reversalKind    WagerTransactionKind
		existingKind    WagerTransactionKind
		existingStatus  WagerTransactionStatus
		wantErr         error
	}{
		// REFUND targets.
		{name: "refund bet processed", referenceKind: TransactionBet, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund},
		{name: "refund win rejected", referenceKind: TransactionWin, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund, wantErr: ErrInvalidReferenceKind},
		{name: "refund loss rejected", referenceKind: TransactionLoss, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund, wantErr: ErrInvalidReferenceKind},
		{name: "refund refund rejected", referenceKind: TransactionRefund, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund, wantErr: ErrInvalidReferenceKind},
		{name: "refund rollback rejected", referenceKind: TransactionRollback, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund, wantErr: ErrInvalidReferenceKind},
		{name: "refund opening rejected", referenceKind: TransactionOpening, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund, wantErr: ErrInvalidReferenceKind},
		{name: "refund bet pending rejected", referenceKind: TransactionBet, referenceStatus: TransactionPending, reversalKind: TransactionRefund, wantErr: ErrReferenceNotProcessed},
		{name: "refund bet pending reference rejected", referenceKind: TransactionBet, referenceStatus: TransactionPendingReference, reversalKind: TransactionRefund, wantErr: ErrReferenceNotProcessed},
		{name: "refund bet rejected rejected", referenceKind: TransactionBet, referenceStatus: TransactionRejected, reversalKind: TransactionRefund, wantErr: ErrReferenceNotProcessed},
		{name: "refund bet failed rejected", referenceKind: TransactionBet, referenceStatus: TransactionFailed, reversalKind: TransactionRefund, wantErr: ErrReferenceNotProcessed},

		// ROLLBACK targets.
		{name: "rollback bet processed", referenceKind: TransactionBet, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback},
		{name: "rollback win processed", referenceKind: TransactionWin, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback},
		{name: "rollback refund processed", referenceKind: TransactionRefund, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback},
		{name: "rollback loss rejected", referenceKind: TransactionLoss, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback, wantErr: ErrInvalidReferenceKind},
		{name: "rollback opening rejected", referenceKind: TransactionOpening, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback, wantErr: ErrInvalidReferenceKind},
		{name: "rollback rollback rejected", referenceKind: TransactionRollback, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback, wantErr: ErrInvalidReferenceKind},
		{name: "rollback bet pending rejected", referenceKind: TransactionBet, referenceStatus: TransactionPending, reversalKind: TransactionRollback, wantErr: ErrReferenceNotProcessed},
		{name: "rollback bet pending reference rejected", referenceKind: TransactionBet, referenceStatus: TransactionPendingReference, reversalKind: TransactionRollback, wantErr: ErrReferenceNotProcessed},
		{name: "rollback win rejected rejected", referenceKind: TransactionWin, referenceStatus: TransactionRejected, reversalKind: TransactionRollback, wantErr: ErrReferenceNotProcessed},
		{name: "rollback win failed rejected", referenceKind: TransactionWin, referenceStatus: TransactionFailed, reversalKind: TransactionRollback, wantErr: ErrReferenceNotProcessed},

		// Double reversal: at most one successful reversal per reference.
		{name: "bet refunded blocks rollback", referenceKind: TransactionBet, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback, existingKind: TransactionRefund, existingStatus: TransactionProcessed, wantErr: ErrDuplicateReversal},
		{name: "bet rolled back blocks refund", referenceKind: TransactionBet, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund, existingKind: TransactionRollback, existingStatus: TransactionProcessed, wantErr: ErrDuplicateReversal},
		{name: "win rolled back blocks second rollback", referenceKind: TransactionWin, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback, existingKind: TransactionRollback, existingStatus: TransactionProcessed, wantErr: ErrDuplicateReversal},
		{name: "refund rolled back blocks second rollback", referenceKind: TransactionRefund, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback, existingKind: TransactionRollback, existingStatus: TransactionProcessed, wantErr: ErrDuplicateReversal},

		// Positive cases without existing reversal.
		{name: "bet processed accepts refund", referenceKind: TransactionBet, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund},
		{name: "bet processed accepts rollback", referenceKind: TransactionBet, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback},
		{name: "win processed accepts rollback", referenceKind: TransactionWin, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback},
		{name: "refund processed accepts rollback", referenceKind: TransactionRefund, referenceStatus: TransactionProcessed, reversalKind: TransactionRollback},

		// Non-successful existing reversal does not block.
		{name: "rejected existing reversal does not block", referenceKind: TransactionBet, referenceStatus: TransactionProcessed, reversalKind: TransactionRefund, existingKind: TransactionRefund, existingStatus: TransactionRejected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reference := reversalFixture(t, "ref-"+tt.name, tt.referenceKind, tt.referenceStatus)
			reversal := reversalFixture(t, "new-"+tt.name, tt.reversalKind, TransactionPending)

			var existing *WagerTransaction
			if tt.existingKind != "" {
				existing = reversalFixture(t, "existing-"+tt.name, tt.existingKind, tt.existingStatus)
			}

			err := ValidateReversal(reference, existing, reversal)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}

	t.Run("non reversal kind is rejected", func(t *testing.T) {
		reference := reversalFixture(t, "ref-bet", TransactionBet, TransactionProcessed)
		reversal := reversalFixture(t, "new-bet", TransactionBet, TransactionPending)

		if err := ValidateReversal(reference, nil, reversal); !errors.Is(err, ErrInvalidTransactionKind) {
			t.Fatalf("expected ErrInvalidTransactionKind, got %v", err)
		}
	})

	t.Run("nil reversal is rejected", func(t *testing.T) {
		reference := reversalFixture(t, "ref-bet", TransactionBet, TransactionProcessed)

		if err := ValidateReversal(reference, nil, nil); err == nil {
			t.Fatal("expected error for nil reversal")
		}
	})

	t.Run("nil reference is rejected", func(t *testing.T) {
		reversal := reversalFixture(t, "new-refund", TransactionRefund, TransactionPending)

		if err := ValidateReversal(nil, nil, reversal); err == nil {
			t.Fatal("expected error for nil reference")
		}
	})
}

func TestWagerTransaction_IsReversal(t *testing.T) {
	for _, kind := range []WagerTransactionKind{TransactionRefund, TransactionRollback} {
		tx := reversalFixture(t, string(kind), kind, TransactionPending)
		if !tx.IsReversal() {
			t.Errorf("kind %q: IsReversal() = false, want true", kind)
		}
	}
	for _, kind := range []WagerTransactionKind{TransactionOpening, TransactionBet, TransactionWin, TransactionLoss} {
		tx := reversalFixture(t, string(kind), kind, TransactionPending)
		if tx.IsReversal() {
			t.Errorf("kind %q: IsReversal() = true, want false", kind)
		}
	}
}

func effectFixture(t *testing.T, idSuffix string, kind WagerTransactionKind, cents int64, reference string) *WagerTransaction {
	t.Helper()

	if reference == "" && (kind == TransactionRefund || kind == TransactionRollback) {
		reference = "ext-dummy-ref"
	}

	tx, err := NewWagerTransaction(
		"tx-"+idSuffix, "prov-1", "ext-"+idSuffix,
		"key-"+idSuffix, "hash-1",
		"player-1", "wallet-1",
		"round-1", "game-1",
		kind, mustMoney(t, cents, "BRL"), reference,
	)
	if err != nil {
		t.Fatalf("NewWagerTransaction(%q): unexpected error: %v", kind, err)
	}
	return tx
}

func TestWagerTransaction_FinancialEffect(t *testing.T) {
	tests := []struct {
		name          string
		kind          WagerTransactionKind
		cents         int64
		referenceKind WagerTransactionKind
		wantMovement  bool
		wantDirection LedgerDirection
		wantCents     int64
		wantErr       error
	}{
		{name: "opening positive is credit", kind: TransactionOpening, cents: 10000, wantMovement: true, wantDirection: LedgerCredit, wantCents: 10000},
		{name: "opening zero has no movement", kind: TransactionOpening, cents: 0, wantMovement: false},
		{name: "bet is debit", kind: TransactionBet, cents: 1000, wantMovement: true, wantDirection: LedgerDebit, wantCents: 1000},
		{name: "win is credit", kind: TransactionWin, cents: 1000, wantMovement: true, wantDirection: LedgerCredit, wantCents: 1000},
		{name: "loss has no movement", kind: TransactionLoss, cents: 0, wantMovement: false},
		{name: "refund is credit", kind: TransactionRefund, cents: 1000, wantMovement: true, wantDirection: LedgerCredit, wantCents: 1000},
		{name: "rollback bet is credit", kind: TransactionRollback, cents: 1000, referenceKind: TransactionBet, wantMovement: true, wantDirection: LedgerCredit, wantCents: 1000},
		{name: "rollback win is debit", kind: TransactionRollback, cents: 1000, referenceKind: TransactionWin, wantMovement: true, wantDirection: LedgerDebit, wantCents: 1000},
		{name: "rollback refund is debit", kind: TransactionRollback, cents: 1000, referenceKind: TransactionRefund, wantMovement: true, wantDirection: LedgerDebit, wantCents: 1000},
		{name: "rollback loss is rejected", kind: TransactionRollback, cents: 1000, referenceKind: TransactionLoss, wantErr: ErrInvalidReferenceKind},
		{name: "rollback opening is rejected", kind: TransactionRollback, cents: 1000, referenceKind: TransactionOpening, wantErr: ErrInvalidReferenceKind},
		{name: "rollback rollback is rejected", kind: TransactionRollback, cents: 1000, referenceKind: TransactionRollback, wantErr: ErrInvalidReferenceKind},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := effectFixture(t, "tx", tt.kind, tt.cents, "ext-ref-1")

			var reference *WagerTransaction
			if tt.kind == TransactionRollback && tt.referenceKind != "" {
				refCents := int64(5000)
				if tt.referenceKind == TransactionLoss {
					refCents = 0
				}
				reference = effectFixture(t, "ref", tt.referenceKind, refCents, "")
			}

			fx, err := tx.FinancialEffect(reference)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if fx.HasMovement != tt.wantMovement {
				t.Fatalf("HasMovement = %v, want %v", fx.HasMovement, tt.wantMovement)
			}
			if !tt.wantMovement {
				if fx.Direction == LedgerDebit && fx.Amount.Cents() == 0 {
					t.Error("no-movement effect must not look like a zero DEBIT")
				}
				if fx.Direction != "" {
					t.Errorf("Direction = %q, want empty on no movement", fx.Direction)
				}
				return
			}
			if fx.Direction != tt.wantDirection {
				t.Errorf("Direction = %q, want %q", fx.Direction, tt.wantDirection)
			}
			if fx.Amount.Cents() != tt.wantCents {
				t.Errorf("Amount = %d, want %d", fx.Amount.Cents(), tt.wantCents)
			}
			if fx.Amount.Currency() != "BRL" {
				t.Errorf("Amount currency = %q, want BRL", fx.Amount.Currency())
			}
		})
	}

	t.Run("rollback nil reference is rejected", func(t *testing.T) {
		tx := effectFixture(t, "tx", TransactionRollback, 1000, "ext-ref-1")

		if _, err := tx.FinancialEffect(nil); err == nil {
			t.Fatal("expected error for nil reference")
		}
	})

	t.Run("repeated calls are deterministic", func(t *testing.T) {
		tx := effectFixture(t, "tx", TransactionRollback, 1000, "ext-ref-1")
		reference := effectFixture(t, "ref", TransactionWin, 5000, "")

		first, err := tx.FinancialEffect(reference)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		second, err := tx.FinancialEffect(reference)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if first != second {
			t.Errorf("effects differ: %+v vs %+v", first, second)
		}
	})

	t.Run("does not mutate transaction or reference", func(t *testing.T) {
		tx := effectFixture(t, "tx", TransactionRollback, 1000, "ext-ref-1")
		reference := effectFixture(t, "ref", TransactionWin, 5000, "")

		beforeTx := *tx
		beforeRef := *reference

		if _, err := tx.FinancialEffect(reference); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if *tx != beforeTx {
			t.Errorf("transaction was mutated: %+v vs %+v", *tx, beforeTx)
		}
		if *reference != beforeRef {
			t.Errorf("reference was mutated: %+v vs %+v", *reference, beforeRef)
		}
	})
}

func TestWagerTransaction_Complete(t *testing.T) {
	t.Run("marks pending as processed", func(t *testing.T) {
		tx := validTransaction(t)

		balance := mustMoney(t, 10000, "BRL")
		if err := tx.Complete(balance); err != nil {
			t.Fatalf("Complete: unexpected error: %v", err)
		}

		if !tx.IsProcessed() {
			t.Errorf("Status = %q, want PROCESSED", tx.Status)
		}
		if tx.ResultingBalance == nil {
			t.Fatal("ResultingBalance is nil after Complete")
		}
		if tx.ResultingBalance.Cents() != 10000 {
			t.Errorf("ResultingBalance = %d, want 10000", tx.ResultingBalance.Cents())
		}
	})

	t.Run("processes from pending reference", func(t *testing.T) {
		tx := validTransaction(t)
		if err := tx.MarkPendingReference(); err != nil {
			t.Fatalf("MarkPendingReference: unexpected error: %v", err)
		}

		if err := tx.Complete(mustMoney(t, 10000, "BRL")); err != nil {
			t.Fatalf("Complete: unexpected error: %v", err)
		}
		if !tx.IsProcessed() {
			t.Errorf("Status = %q, want PROCESSED", tx.Status)
		}
	})

	t.Run("rejects currency mismatch", func(t *testing.T) {
		tx := validTransaction(t)

		err := tx.Complete(mustMoney(t, 10000, "USD"))
		if !errors.Is(err, ErrCurrencyMismatch) {
			t.Fatalf("expected ErrCurrencyMismatch, got %v", err)
		}
		if !tx.IsPending() {
			t.Errorf("Status = %q, want PENDING", tx.Status)
		}
		if tx.ResultingBalance != nil {
			t.Errorf("ResultingBalance = %+v, want nil", tx.ResultingBalance)
		}
	})

	t.Run("rejects complete on processed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		err := tx.Complete(mustMoney(t, 2000, "BRL"))
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects complete on rejected", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Reject("INSUFFICIENT_FUNDS")

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

	t.Run("fails from pending reference", func(t *testing.T) {
		tx := validTransaction(t)
		if err := tx.MarkPendingReference(); err != nil {
			t.Fatalf("MarkPendingReference: unexpected error: %v", err)
		}

		if err := tx.Fail("INFRA_TIMEOUT"); err != nil {
			t.Fatalf("Fail: unexpected error: %v", err)
		}
		if !tx.IsFailed() {
			t.Errorf("Status = %q, want FAILED", tx.Status)
		}
	})

	t.Run("rejects fail on processed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		err := tx.Fail("SOME_ERROR")
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects fail on rejected", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Reject("INSUFFICIENT_FUNDS")

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

func TestWagerTransaction_Reject(t *testing.T) {
	t.Run("marks pending as rejected", func(t *testing.T) {
		tx := validTransaction(t)

		if err := tx.Reject("INSUFFICIENT_FUNDS"); err != nil {
			t.Fatalf("Reject: unexpected error: %v", err)
		}

		if !tx.IsRejected() {
			t.Errorf("Status = %q, want REJECTED", tx.Status)
		}
		if tx.FailureCode != "INSUFFICIENT_FUNDS" {
			t.Errorf("FailureCode = %q, want INSUFFICIENT_FUNDS", tx.FailureCode)
		}
	})

	t.Run("rejects from pending reference", func(t *testing.T) {
		tx := validTransaction(t)
		if err := tx.MarkPendingReference(); err != nil {
			t.Fatalf("MarkPendingReference: unexpected error: %v", err)
		}

		if err := tx.Reject("REFERENCE_NOT_FOUND"); err != nil {
			t.Fatalf("Reject: unexpected error: %v", err)
		}
		if !tx.IsRejected() {
			t.Errorf("Status = %q, want REJECTED", tx.Status)
		}
	})

	t.Run("rejects empty failure code", func(t *testing.T) {
		tx := validTransaction(t)

		err := tx.Reject("")
		if err == nil {
			t.Fatal("expected error for empty failure code")
		}

		// Status should remain pending
		if !tx.IsPending() {
			t.Errorf("Status = %q, want PENDING", tx.Status)
		}
	})

	t.Run("rejects reject on processed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		err := tx.Reject("SOME_ERROR")
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects reject on already rejected", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Reject("FIRST_ERROR")

		err := tx.Reject("SECOND_ERROR")
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects reject on failed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Fail("INFRA_TIMEOUT")

		err := tx.Reject("SOME_ERROR")
		if !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})
}

func TestWagerTransaction_MarkPendingReference(t *testing.T) {
	t.Run("marks pending as pending reference", func(t *testing.T) {
		tx := validTransaction(t)

		if err := tx.MarkPendingReference(); err != nil {
			t.Fatalf("MarkPendingReference: unexpected error: %v", err)
		}

		if !tx.IsPendingReference() {
			t.Errorf("Status = %q, want PENDING_REFERENCE", tx.Status)
		}
	})

	t.Run("rejects on processed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		if err := tx.MarkPendingReference(); !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects on rejected", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Reject("INSUFFICIENT_FUNDS")

		if err := tx.MarkPendingReference(); !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects on failed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Fail("INFRA_TIMEOUT")

		if err := tx.MarkPendingReference(); !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})

	t.Run("rejects on already pending reference", func(t *testing.T) {
		tx := validTransaction(t)
		if err := tx.MarkPendingReference(); err != nil {
			t.Fatalf("MarkPendingReference: unexpected error: %v", err)
		}

		if err := tx.MarkPendingReference(); !errors.Is(err, ErrTransactionNotPending) {
			t.Fatalf("expected ErrTransactionNotPending, got %v", err)
		}
	})
}

func TestWagerTransaction_StatusPredicates(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		tx := validTransaction(t)

		if !tx.IsPending() {
			t.Error("IsPending() = false, want true")
		}
		if tx.IsPendingReference() {
			t.Error("IsPendingReference() = true, want false")
		}
		if tx.IsProcessed() {
			t.Error("IsProcessed() = true, want false")
		}
		if tx.IsRejected() {
			t.Error("IsRejected() = true, want false")
		}
		if tx.IsFailed() {
			t.Error("IsFailed() = true, want false")
		}
	})

	t.Run("pending reference", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.MarkPendingReference()

		if tx.IsPending() {
			t.Error("IsPending() = true, want false")
		}
		if !tx.IsPendingReference() {
			t.Error("IsPendingReference() = false, want true")
		}
		if tx.IsProcessed() {
			t.Error("IsProcessed() = true, want false")
		}
		if tx.IsRejected() {
			t.Error("IsRejected() = true, want false")
		}
		if tx.IsFailed() {
			t.Error("IsFailed() = true, want false")
		}
	})

	t.Run("processed", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Complete(mustMoney(t, 1000, "BRL"))

		if tx.IsPending() {
			t.Error("IsPending() = true, want false")
		}
		if tx.IsPendingReference() {
			t.Error("IsPendingReference() = true, want false")
		}
		if !tx.IsProcessed() {
			t.Error("IsProcessed() = false, want true")
		}
		if tx.IsRejected() {
			t.Error("IsRejected() = true, want false")
		}
		if tx.IsFailed() {
			t.Error("IsFailed() = true, want false")
		}
	})

	t.Run("rejected", func(t *testing.T) {
		tx := validTransaction(t)
		_ = tx.Reject("INSUFFICIENT_FUNDS")

		if tx.IsPending() {
			t.Error("IsPending() = true, want false")
		}
		if tx.IsPendingReference() {
			t.Error("IsPendingReference() = true, want false")
		}
		if tx.IsProcessed() {
			t.Error("IsProcessed() = true, want false")
		}
		if !tx.IsRejected() {
			t.Error("IsRejected() = false, want true")
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
		if tx.IsPendingReference() {
			t.Error("IsPendingReference() = true, want false")
		}
		if tx.IsProcessed() {
			t.Error("IsProcessed() = true, want false")
		}
		if tx.IsRejected() {
			t.Error("IsRejected() = true, want false")
		}
		if !tx.IsFailed() {
			t.Error("IsFailed() = false, want true")
		}
	})
}
