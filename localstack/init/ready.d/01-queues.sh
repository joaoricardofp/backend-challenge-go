#!/bin/bash
# Cria as filas SQS locais (B3.13): entrada de wagering, eventos da outbox e
# DLQ de eventos. Executado pelo LocalStack após o gateway ficar pronto
# (init/ready.d). Idempotente: create-queue com o mesmo nome é no-op.
set -e
awslocal sqs create-queue --queue-name wager-input
awslocal sqs create-queue --queue-name wager-events
awslocal sqs create-queue --queue-name wager-events-dlq
