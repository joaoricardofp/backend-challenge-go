package main

import (
	"go.uber.org/fx"

	"github.com/joaoricardofp/backend-challenge-go/internal/app"
)

func main() {
	// fx.Run trata SIGINT/SIGTERM e orquestra start/stop com lifecycle.
	// Toda a composição vive em internal/app; aqui não há wiring manual.
	fx.New(app.Module).Run()
}
