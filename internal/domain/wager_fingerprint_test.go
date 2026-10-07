package domain

import (
	"encoding/json"
	"regexp"
	"testing"
)

func fingerprintFixture(t *testing.T) WagerTransaction {
	t.Helper()

	return WagerTransaction{
		ID:                    "tx-internal-1",
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PayloadHash:           "",
		PlayerID:              "player-1",
		WalletID:              "wallet-1",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  TransactionBet,
		Status:                TransactionPending,
		Amount:                mustMoney(t, 2500, "BRL"),
	}
}

func mustFingerprint(t *testing.T, tx WagerTransaction) string {
	t.Helper()

	fp, err := CanonicalWagerFingerprint(tx)
	if err != nil {
		t.Fatalf("CanonicalWagerFingerprint: unexpected error: %v", err)
	}
	return fp
}

func TestCanonicalWagerFingerprint_CanonicalJSON(t *testing.T) {
	tx := fingerprintFixture(t)

	raw, err := json.Marshal(canonicalPayload(tx))
	if err != nil {
		t.Fatalf("marshal: unexpected error: %v", err)
	}

	want := `{"providerId":"provider-a","externalTransactionId":"transaction-123",` +
		`"playerId":"player-1","walletId":"wallet-1",` +
		`"roundId":"round-987","gameId":"fortune-chimp",` +
		`"kind":"BET","amount":"25.00","currency":"BRL",` +
		`"referenceExternalTransactionId":null}`

	if string(raw) != want {
		t.Errorf("canonical JSON = %s, want %s", raw, want)
	}
}

func TestCanonicalWagerFingerprint_KnownAnswer(t *testing.T) {
	const want = "b10dce1a3976cb9a010fc62d6325c02e69d96c4ac784fc34f255f7f5e6c25be9"

	if got := mustFingerprint(t, fingerprintFixture(t)); got != want {
		t.Errorf("fingerprint = %s, want %s", got, want)
	}
}

func TestCanonicalWagerFingerprint_Deterministic(t *testing.T) {
	tx := fingerprintFixture(t)

	first := mustFingerprint(t, tx)
	second := mustFingerprint(t, tx)

	if first != second {
		t.Errorf("same input produced different fingerprints: %s vs %s", first, second)
	}

	matched, err := regexp.MatchString(`^[0-9a-f]{64}$`, first)
	if err != nil {
		t.Fatalf("regexp: unexpected error: %v", err)
	}
	if !matched {
		t.Errorf("fingerprint = %q, want 64 lowercase hex chars", first)
	}
}

func TestCanonicalWagerFingerprint_Fields(t *testing.T) {
	base := mustFingerprint(t, fingerprintFixture(t))
	amount2600 := mustMoney(t, 2600, "BRL")
	amountUSD := mustMoney(t, 2500, "USD")

	mutate := func(t *testing.T, f func(*WagerTransaction)) string {
		t.Helper()
		tx := fingerprintFixture(t)
		f(&tx)
		return mustFingerprint(t, tx)
	}

	different := []struct {
		name   string
		mutate func(*WagerTransaction)
	}{
		{name: "providerId", mutate: func(tx *WagerTransaction) { tx.ProviderID = "provider-b" }},
		{name: "externalTransactionId", mutate: func(tx *WagerTransaction) { tx.ExternalTransactionID = "transaction-999" }},
		{name: "playerId", mutate: func(tx *WagerTransaction) { tx.PlayerID = "player-2" }},
		{name: "walletId", mutate: func(tx *WagerTransaction) { tx.WalletID = "wallet-2" }},
		{name: "roundId", mutate: func(tx *WagerTransaction) { tx.RoundID = "round-000" }},
		{name: "gameId", mutate: func(tx *WagerTransaction) { tx.GameID = "other-game" }},
		{name: "kind", mutate: func(tx *WagerTransaction) { tx.Kind = TransactionWin }},
		{name: "amount", mutate: func(tx *WagerTransaction) { tx.Amount = amount2600 }},
		{name: "currency", mutate: func(tx *WagerTransaction) { tx.Amount = amountUSD }},
		{name: "reference", mutate: func(tx *WagerTransaction) { tx.ReferenceExternalTransactionID = "ext-bet-9" }},
		{name: "roundId emptied", mutate: func(tx *WagerTransaction) { tx.RoundID = "" }},
		{name: "reference emptied", mutate: func(tx *WagerTransaction) {
			tx.Kind = TransactionRefund
			tx.ReferenceExternalTransactionID = ""
		}},
	}

	for _, tt := range different {
		t.Run(tt.name, func(t *testing.T) {
			if got := mutate(t, tt.mutate); got == base {
				t.Error("different input produced the same fingerprint")
			}
		})
	}

	same := []struct {
		name   string
		mutate func(*WagerTransaction)
	}{
		{name: "idempotencyKey", mutate: func(tx *WagerTransaction) { tx.IdempotencyKey = "totally-different-key" }},
		{name: "status", mutate: func(tx *WagerTransaction) { tx.Status = TransactionProcessed }},
		{name: "database ID", mutate: func(tx *WagerTransaction) { tx.ID = "other-internal-id" }},
		{name: "payloadHash", mutate: func(tx *WagerTransaction) { tx.PayloadHash = "some-hash" }},
		{name: "resultingBalance", mutate: func(tx *WagerTransaction) {
			b := tx.Amount
			tx.ResultingBalance = &b
		}},
		{name: "failureCode", mutate: func(tx *WagerTransaction) { tx.FailureCode = "SOME_CODE" }},
	}

	for _, tt := range same {
		t.Run(tt.name+" excluded", func(t *testing.T) {
			if got := mutate(t, tt.mutate); got != base {
				t.Errorf("excluded field changed fingerprint: %s vs %s", got, base)
			}
		})
	}
}

func TestCanonicalWagerFingerprint_Money(t *testing.T) {
	t.Run("decimal string round-trips through fingerprint", func(t *testing.T) {
		fromCents := fingerprintFixture(t)

		parsed, err := NewMoneyFromDecimal("25.00", "BRL")
		if err != nil {
			t.Fatalf("NewMoneyFromDecimal: unexpected error: %v", err)
		}
		fromDecimal := fingerprintFixture(t)
		fromDecimal.Amount = parsed

		if mustFingerprint(t, fromCents) != mustFingerprint(t, fromDecimal) {
			t.Error("2500 cents and \"25.00\" produced different fingerprints")
		}
	})

	t.Run("amount keeps two decimals", func(t *testing.T) {
		tx := fingerprintFixture(t)
		raw, err := json.Marshal(canonicalPayload(tx))
		if err != nil {
			t.Fatalf("marshal: unexpected error: %v", err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: unexpected error: %v", err)
		}
		if decoded["amount"] != "25.00" {
			t.Errorf("amount = %v, want \"25.00\"", decoded["amount"])
		}
		if decoded["currency"] != "BRL" {
			t.Errorf("currency = %v, want \"BRL\"", decoded["currency"])
		}
	})
}

func TestCanonicalWagerFingerprint_OptionalFields(t *testing.T) {
	t.Run("empty optional serializes as null", func(t *testing.T) {
		tx := fingerprintFixture(t)
		tx.RoundID = ""
		tx.GameID = ""
		tx.ReferenceExternalTransactionID = ""

		raw, err := json.Marshal(canonicalPayload(tx))
		if err != nil {
			t.Fatalf("marshal: unexpected error: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: unexpected error: %v", err)
		}
		for _, field := range []string{"roundId", "gameId", "referenceExternalTransactionId"} {
			if decoded[field] != nil {
				t.Errorf("%s = %v, want null (empty means absent)", field, decoded[field])
			}
		}
	})

	t.Run("empty vs set are different fingerprints", func(t *testing.T) {
		withRef := fingerprintFixture(t)
		withRef.ReferenceExternalTransactionID = "ext-bet-1"

		if mustFingerprint(t, fingerprintFixture(t)) == mustFingerprint(t, withRef) {
			t.Error("absent and set reference produced the same fingerprint")
		}
	})
}

func TestCanonicalWagerFingerprint_DoesNotMutate(t *testing.T) {
	tx := fingerprintFixture(t)
	before := tx

	if _, err := CanonicalWagerFingerprint(tx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx != before {
		t.Errorf("input was mutated: %+v vs %+v", tx, before)
	}
}
