package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// Testes de fronteira B3.5: o caso de uso ProcessWager é a única operação de
// application para wagers, com entrada/saída independentes de transporte.
// HTTP e SQS (futuros) devem construir o mesmo ProcessWagerInput e chamar o
// mesmo método. Nenhum mock fictício de transporte: os adaptadores abaixo
// são apenas envelopes locais que provam a construção convergente.

// httpEnvelope simula o que um handler HTTP futuro extrairá da requisição:
// campos de negócio + chave vinda de header. Não é usado pela application.
type httpEnvelope struct {
	providerID string
	externalID string
	key        string
	playerID   string
	walletID   string
	kind       domain.WagerTransactionKind
	cents      int64
}

// sqsEnvelope simula o que um consumer SQS futuro extrairá da mensagem:
// mesmos campos de negócio vindos do corpo JSON. Não é usado pela application.
type sqsEnvelope struct {
	providerID string
	externalID string
	key        string
	playerID   string
	walletID   string
	kind       domain.WagerTransactionKind
	cents      int64
}

func buildTx(t *testing.T, f *serviceFixture, providerID, externalID, key, playerID, walletID string, kind domain.WagerTransactionKind, cents int64) domain.WagerTransaction {
	t.Helper()

	amount, err := domain.NewMoney(cents, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), providerID, externalID, key, "hash-test",
		playerID, walletID, "round-1", "game-1", kind, amount, "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	return *wt
}

// adaptHTTP e adaptSQS convergem para o mesmo ProcessWagerInput a partir de
// envelopes distintos: a application só enxerga o input, nunca o transporte.
func adaptHTTP(t *testing.T, f *serviceFixture, e httpEnvelope) application.ProcessWagerInput {
	t.Helper()
	return application.ProcessWagerInput{Transaction: buildTx(t, f, e.providerID, e.externalID, e.key, e.playerID, e.walletID, e.kind, e.cents)}
}

func adaptSQS(t *testing.T, f *serviceFixture, e sqsEnvelope) application.ProcessWagerInput {
	t.Helper()
	return application.ProcessWagerInput{Transaction: buildTx(t, f, e.providerID, e.externalID, e.key, e.playerID, e.walletID, e.kind, e.cents)}
}

func TestApplicationBoundary_SingleUseCaseForAllTransports(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	providerID := testUUID(t, f.pool)
	extTxID := "ext-" + testUUID(t, f.pool)
	key := "key-" + testUUID(t, f.pool)

	// "HTTP" processa primeiro: operação nova.
	httpRes, err := f.service.Process(context.Background(), adaptHTTP(t, f, httpEnvelope{
		providerID: providerID, externalID: extTxID, key: key,
		playerID: wallet.PlayerID, walletID: wallet.ID,
		kind:  domain.TransactionBet,
		cents: 3000,
	}))
	if err != nil {
		t.Fatalf("http adapter process: unexpected error: %v", err)
	}
	if httpRes.Replayed {
		t.Error("first processing marked as replay")
	}

	// "SQS" entrega a mesma operação lógica depois: replay, sem novo efeito.
	sqsRes, err := f.service.Process(context.Background(), adaptSQS(t, f, sqsEnvelope{
		providerID: providerID, externalID: extTxID, key: key,
		playerID: wallet.PlayerID, walletID: wallet.ID,
		kind:  domain.TransactionBet,
		cents: 3000,
	}))
	if err != nil {
		t.Fatalf("sqs adapter process: unexpected error: %v", err)
	}
	if !sqsRes.Replayed {
		t.Error("same operation via other adapter not recognized as replay")
	}
	if sqsRes.Transaction.ID != httpRes.Transaction.ID {
		t.Errorf("replay ID = %q, want %q", sqsRes.Transaction.ID, httpRes.Transaction.ID)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 7000 || stored.Version != 2 {
		t.Errorf("wallet = %d/v%d, want 7000/v2 (single effect)", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Errorf("ledger entries = %d, want 1", len(entries))
	}
}

func TestApplicationBoundary_ResultShape(t *testing.T) {
	// Tripwire intencional: o resultado da application só pode carregar
	// dados de aplicação/domínio. Adicionar campo de transporte aqui exige
	// atualizar este teste de propósito.
	inputFields := fieldNames(t, reflect.TypeOf(application.ProcessWagerInput{}))
	assertFieldSet(t, "ProcessWagerInput", inputFields, []string{"Transaction"})

	resultFields := fieldNames(t, reflect.TypeOf(application.ProcessWagerResult{}))
	assertFieldSet(t, "ProcessWagerResult", resultFields, []string{"Transaction", "Balance", "Replayed"})

	inputType := reflect.TypeOf(application.ProcessWagerInput{})
	if inputType.Field(0).Type != reflect.TypeOf(domain.WagerTransaction{}) {
		t.Errorf("ProcessWagerInput.Transaction type = %v, want domain.WagerTransaction", inputType.Field(0).Type)
	}
}

func fieldNames(t *testing.T, rt reflect.Type) []string {
	t.Helper()

	var names []string
	for i := 0; i < rt.NumField(); i++ {
		names = append(names, rt.Field(i).Name)
	}
	return names
}

func assertFieldSet(t *testing.T, what string, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s fields = %v, want exactly %v", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s fields = %v, want exactly %v", what, got, want)
		}
	}
}

func TestApplicationBoundary_ErrorsStaySemantic(t *testing.T) {
	// Erros da application são sentinelas de domínio/application, sem status
	// HTTP ou envelope de transporte: o mapeamento é dever do transporte.
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "USD")

	_, err := f.service.Process(context.Background(), input)
	if !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("error = %v, want ErrCurrencyMismatch", err)
	}
	var httpErr interface{ StatusCode() int }
	if errors.As(err, &httpErr) {
		t.Errorf("application error exposes transport behavior: %T", err)
	}
}
