package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
	wagerhttp "github.com/joaoricardofp/backend-challenge-go/internal/transport/http"
)

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/backend-challenge-go"
	}

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	service := application.NewWagerService(pool, wallets, transactions, ledger)
	wagerHandler := wagerhttp.NewWagerHandler(service)

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("POST /wagering/transactions", wagerHandler.ProcessWager)

	log.Printf("Server started on :8080")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
