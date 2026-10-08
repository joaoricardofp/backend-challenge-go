package config

import (
	"testing"
)

func TestSQSConfig_Defaults(t *testing.T) {
	t.Setenv("SQS_ENABLED", "")
	t.Setenv("SQS_WAGER_QUEUE_URL", "")
	t.Setenv("SQS_OUTBOX_QUEUE_URL", "")
	t.Setenv("SQS_OUTBOX_DLQ_URL", "")
	t.Setenv("SQS_CONSUMER_NAME", "")
	t.Setenv("SQS_REGION", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("SQS_ENDPOINT", "")
	t.Setenv("SQS_MAX_MESSAGES", "")
	t.Setenv("SQS_WAIT_TIME_SECONDS", "")
	t.Setenv("SQS_VISIBILITY_TIMEOUT_SECONDS", "")
	t.Setenv("SQS_OUTBOX_POLL_INTERVAL_SECONDS", "")
	t.Setenv("SQS_OUTBOX_BATCH_SIZE", "")
	t.Setenv("SQS_OUTBOX_MAX_ATTEMPTS", "")
	t.Setenv("SQS_OUTBOX_MAX_BACKOFF_SECONDS", "")

	cfg := SQSConfigFromEnv()
	if cfg.Enabled {
		t.Error("Enabled = true, want false (opt-in)")
	}
	if cfg.WagerQueueURL != "" || cfg.OutboxQueueURL != "" || cfg.OutboxDLQURL != "" {
		t.Errorf("queue URLs = %q/%q/%q, want empty",
			cfg.WagerQueueURL, cfg.OutboxQueueURL, cfg.OutboxDLQURL)
	}
	if cfg.ConsumerName != "wager-consumer" {
		t.Errorf("ConsumerName = %q, want wager-consumer", cfg.ConsumerName)
	}
	if cfg.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", cfg.Region)
	}
	if cfg.MaxMessages != 10 || cfg.WaitTimeSeconds != 20 || cfg.VisibilityTimeoutSecond != 30 {
		t.Errorf("polling defaults = %d/%d/%d, want 10/20/30",
			cfg.MaxMessages, cfg.WaitTimeSeconds, cfg.VisibilityTimeoutSecond)
	}
	if cfg.OutboxPollIntervalSeconds != 1 || cfg.OutboxBatchSize != 10 {
		t.Errorf("outbox defaults = %d/%d, want 1/10",
			cfg.OutboxPollIntervalSeconds, cfg.OutboxBatchSize)
	}
	if cfg.OutboxMaxAttempts != 5 || cfg.OutboxMaxBackoffSeconds != 300 {
		t.Errorf("retry defaults = %d/%d, want 5/300",
			cfg.OutboxMaxAttempts, cfg.OutboxMaxBackoffSeconds)
	}
	if cfg.ConsumerEnabled() {
		t.Error("ConsumerEnabled = true, want false without wager queue URL")
	}
	if cfg.PublisherEnabled() {
		t.Error("PublisherEnabled = true, want false without outbox queue URL")
	}
}

func TestPendingResolverConfig_Defaults(t *testing.T) {
	t.Setenv("PENDING_RESOLVER_ENABLED", "")
	t.Setenv("PENDING_RESOLVER_INTERVAL_SECONDS", "")
	t.Setenv("PENDING_RESOLVER_BATCH_SIZE", "")
	t.Setenv("PENDING_RESOLVER_MAX_ATTEMPTS", "")
	t.Setenv("PENDING_RESOLVER_MAX_BACKOFF_SECONDS", "")

	cfg := PendingResolverConfigFromEnv()
	if !cfg.PendingResolverEnabled() {
		t.Error("Enabled = false, want true (worker exigido pelo README §7)")
	}
	if cfg.IntervalSeconds != 5 || cfg.BatchSize != 10 {
		t.Errorf("poll defaults = %d/%d, want 5/10",
			cfg.IntervalSeconds, cfg.BatchSize)
	}
	if cfg.MaxAttempts != 10 || cfg.MaxBackoffSecond != 300 {
		t.Errorf("retry defaults = %d/%d, want 10/300",
			cfg.MaxAttempts, cfg.MaxBackoffSecond)
	}
}

func TestPendingResolverConfig_ClampAndDisable(t *testing.T) {
	t.Setenv("PENDING_RESOLVER_ENABLED", "false")
	t.Setenv("PENDING_RESOLVER_INTERVAL_SECONDS", "0")
	t.Setenv("PENDING_RESOLVER_BATCH_SIZE", "5000")
	t.Setenv("PENDING_RESOLVER_MAX_ATTEMPTS", "0")
	t.Setenv("PENDING_RESOLVER_MAX_BACKOFF_SECONDS", "99999")

	cfg := PendingResolverConfigFromEnv()
	if cfg.PendingResolverEnabled() {
		t.Error("Enabled = true, want false")
	}
	if cfg.IntervalSeconds != 1 {
		t.Errorf("IntervalSeconds = %d, want clamped to 1", cfg.IntervalSeconds)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want clamped to 100", cfg.BatchSize)
	}
	if cfg.MaxAttempts != 1 {
		t.Errorf("MaxAttempts = %d, want clamped to 1", cfg.MaxAttempts)
	}
	if cfg.MaxBackoffSecond != 3600 {
		t.Errorf("MaxBackoffSecond = %d, want clamped to 3600", cfg.MaxBackoffSecond)
	}
}

func TestSQSConfig_QueueGates(t *testing.T) {
	// Gate geral desligado: nada inicia, mesmo com filas presentes.
	t.Setenv("SQS_ENABLED", "false")
	t.Setenv("SQS_WAGER_QUEUE_URL", "https://example/wager")
	t.Setenv("SQS_OUTBOX_QUEUE_URL", "https://example/events")
	if cfg := SQSConfigFromEnv(); cfg.ConsumerEnabled() || cfg.PublisherEnabled() {
		t.Error("disabled flag with queue URLs should enable nothing")
	}

	// Cada fila habilita somente o seu componente.
	t.Setenv("SQS_ENABLED", "true")
	t.Setenv("SQS_WAGER_QUEUE_URL", "")
	t.Setenv("SQS_OUTBOX_QUEUE_URL", "https://example/events")
	if cfg := SQSConfigFromEnv(); cfg.ConsumerEnabled() || !cfg.PublisherEnabled() {
		t.Error("outbox URL alone should enable only the publisher")
	}

	t.Setenv("SQS_WAGER_QUEUE_URL", "https://example/wager")
	t.Setenv("SQS_OUTBOX_QUEUE_URL", "")
	if cfg := SQSConfigFromEnv(); !cfg.ConsumerEnabled() || cfg.PublisherEnabled() {
		t.Error("wager URL alone should enable only the consumer")
	}

	t.Setenv("SQS_OUTBOX_QUEUE_URL", "https://example/events")
	if cfg := SQSConfigFromEnv(); !cfg.ConsumerEnabled() || !cfg.PublisherEnabled() {
		t.Error("both URLs should enable consumer and publisher")
	}
}

func TestSQSConfig_Clamping(t *testing.T) {
	t.Setenv("SQS_ENABLED", "true")
	t.Setenv("SQS_WAGER_QUEUE_URL", "https://example/wager")
	t.Setenv("SQS_OUTBOX_QUEUE_URL", "https://example/events")
	t.Setenv("SQS_MAX_MESSAGES", "99")
	t.Setenv("SQS_WAIT_TIME_SECONDS", "-5")
	t.Setenv("SQS_VISIBILITY_TIMEOUT_SECONDS", "999999")
	t.Setenv("SQS_OUTBOX_POLL_INTERVAL_SECONDS", "0")
	t.Setenv("SQS_OUTBOX_BATCH_SIZE", "5000")
	t.Setenv("SQS_OUTBOX_MAX_ATTEMPTS", "0")
	t.Setenv("SQS_OUTBOX_MAX_BACKOFF_SECONDS", "99999")

	cfg := SQSConfigFromEnv()
	if cfg.MaxMessages != 10 {
		t.Errorf("MaxMessages = %d, want clamped to 10", cfg.MaxMessages)
	}
	if cfg.WaitTimeSeconds != 0 {
		t.Errorf("WaitTimeSeconds = %d, want clamped to 0", cfg.WaitTimeSeconds)
	}
	if cfg.VisibilityTimeoutSecond != 43200 {
		t.Errorf("VisibilityTimeout = %d, want clamped to 43200", cfg.VisibilityTimeoutSecond)
	}
	if cfg.OutboxPollIntervalSeconds != 1 {
		t.Errorf("OutboxPollInterval = %d, want clamped to 1", cfg.OutboxPollIntervalSeconds)
	}
	if cfg.OutboxBatchSize != 100 {
		t.Errorf("OutboxBatchSize = %d, want clamped to 100", cfg.OutboxBatchSize)
	}
	if cfg.OutboxMaxAttempts != 1 {
		t.Errorf("OutboxMaxAttempts = %d, want clamped to 1", cfg.OutboxMaxAttempts)
	}
	if cfg.OutboxMaxBackoffSeconds != 3600 {
		t.Errorf("OutboxMaxBackoffSeconds = %d, want clamped to 3600", cfg.OutboxMaxBackoffSeconds)
	}
}
