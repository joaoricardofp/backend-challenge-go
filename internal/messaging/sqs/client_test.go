package sqs

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/joaoricardofp/backend-challenge-go/internal/config"
)

// fakeSQSAPI captura as entradas do SDK sem AWS/LocalStack.
type fakeSQSAPI struct {
	mu            sync.Mutex
	sendInputs    []*awssqs.SendMessageInput
	sendErr       error
	attrInputs    []*awssqs.GetQueueAttributesInput
	attrErr       error
	receiveOutput *awssqs.ReceiveMessageOutput
	receiveErr    error
	deleteErr     error
}

func (f *fakeSQSAPI) ReceiveMessage(_ context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	if f.receiveErr != nil {
		return nil, f.receiveErr
	}
	if f.receiveOutput != nil {
		return f.receiveOutput, nil
	}
	return &awssqs.ReceiveMessageOutput{}, nil
}

func (f *fakeSQSAPI) DeleteMessage(_ context.Context, _ *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQSAPI) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendInputs = append(f.sendInputs, in)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	return &awssqs.SendMessageOutput{}, nil
}

func (f *fakeSQSAPI) GetQueueAttributes(_ context.Context, in *awssqs.GetQueueAttributesInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attrInputs = append(f.attrInputs, in)
	if f.attrErr != nil {
		return nil, f.attrErr
	}
	return &awssqs.GetQueueAttributesOutput{}, nil
}

func TestSQSAdapter_SendMessage(t *testing.T) {
	fake := &fakeSQSAPI{}
	a := &SQSAdapter{client: fake, queueURL: "https://sqs.us-east-1.amazonaws.com/123/wager"}

	body := []byte(`{"eventId":"e1","eventType":"WagerTransactionProcessed"}`)
	if err := a.SendMessage(context.Background(), body); err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(fake.sendInputs) != 1 {
		t.Fatalf("sends = %d, want 1", len(fake.sendInputs))
	}
	in := fake.sendInputs[0]
	if in.QueueUrl == nil || *in.QueueUrl != "https://sqs.us-east-1.amazonaws.com/123/wager" {
		t.Errorf("queue URL = %v, want a fila configurada", in.QueueUrl)
	}
	if in.MessageBody == nil || *in.MessageBody != string(body) {
		t.Errorf("body = %v, want o payload byte a byte", in.MessageBody)
	}
	// Fila padrão nesta etapa: sem campos FIFO.
	if in.MessageGroupId != nil || in.MessageDeduplicationId != nil {
		t.Errorf("FIFO fields set: group=%v dedup=%v, want nil", in.MessageGroupId, in.MessageDeduplicationId)
	}
}

func TestSQSAdapter_SendMessageError(t *testing.T) {
	fake := &fakeSQSAPI{sendErr: errors.New("sqs down")}
	a := &SQSAdapter{client: fake, queueURL: "https://example/queue"}

	err := a.SendMessage(context.Background(), []byte(`{}`))
	if err == nil || !errors.Is(err, fake.sendErr) {
		t.Fatalf("error = %v, want o erro do SDK propagado", err)
	}
}

func TestSQSAdapter_RequiresQueueURL(t *testing.T) {
	if _, err := NewSQSAdapter(context.Background(), "", "us-east-1", ""); err == nil {
		t.Fatal("expected error without queue URL")
	}
}

func TestSQSAdapter_CheckQueue(t *testing.T) {
	fake := &fakeSQSAPI{}
	a := &SQSAdapter{client: fake, queueURL: "https://example/queue"}

	if err := a.CheckQueue(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(fake.attrInputs) != 1 {
		t.Fatalf("attr calls = %d, want 1", len(fake.attrInputs))
	}
	in := fake.attrInputs[0]
	if in.QueueUrl == nil || *in.QueueUrl != "https://example/queue" {
		t.Errorf("queue URL = %v, want a fila do adapter", in.QueueUrl)
	}
	if len(in.AttributeNames) != 1 || in.AttributeNames[0] != awstypes.QueueAttributeNameQueueArn {
		t.Errorf("attributes = %v, want somente QueueArn (barato, sem mutação)", in.AttributeNames)
	}

	fake.attrErr = errors.New("sqs down")
	if err := a.CheckQueue(context.Background()); err == nil {
		t.Fatal("expected error when SQS is down")
	}
}

func TestNewQueueAdapters_Disabled(t *testing.T) {
	// Desligado: sem adapters e sem tocar em credencial AWS.
	q, err := NewQueueAdapters(context.Background(), config.SQSConfig{})
	if err != nil {
		t.Fatalf("adapters: %v", err)
	}
	if q.Wager != nil || q.Outbox != nil || q.OutboxDLQ != nil {
		t.Errorf("adapters = %+v, want all nil when disabled", q)
	}
}

func TestNewQueueAdapters_PartialQueues(t *testing.T) {
	// Só a fila de eventos: apenas o adapter de saída existe.
	q, err := NewQueueAdapters(context.Background(), config.SQSConfig{
		Enabled:        true,
		OutboxQueueURL: "https://example/events",
		Region:         "us-east-1",
	})
	if err != nil {
		t.Fatalf("adapters: %v", err)
	}
	if q.Wager != nil {
		t.Error("wager adapter should be nil without wager queue URL")
	}
	if q.Outbox == nil {
		t.Fatal("outbox adapter should exist with outbox queue URL")
	}
	if q.OutboxDLQ != nil {
		t.Error("DLQ adapter should be nil without DLQ URL")
	}
	// Prova a fila correta no SendMessage contra o fake subjacente.
	fake := &fakeSQSAPI{}
	q.Outbox.client = fake
	if err := q.Outbox.SendMessage(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(fake.sendInputs) != 1 || *fake.sendInputs[0].QueueUrl != "https://example/events" {
		t.Errorf("sendInputs = %+v, want a fila de eventos", fake.sendInputs)
	}
}

// TestSQSAdapter_LocalStackTopology prova a topologia B3.13 contra SQS real
// (wager-events + wager-events-dlq criadas pelo init do compose): send →
// receive → delete com corpo idêntico nas duas filas. Opt-in via
// SQS_LOCALSTACK_TEST=1 para nunca tornar a suíte dependente de serviço
// externo; sem a variável, pula em milissegundos.
func TestSQSAdapter_LocalStackTopology(t *testing.T) {
	if os.Getenv("SQS_LOCALSTACK_TEST") == "" {
		t.Skip("set SQS_LOCALSTACK_TEST=1 with localstack running to exercise real SQS")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	endpoint := os.Getenv("SQS_LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	account := "http://localhost:4566/000000000000/"
	if v := os.Getenv("SQS_LOCALSTACK_ACCOUNT"); v != "" {
		account = v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	events, err := NewSQSAdapter(ctx, account+"wager-events", "us-east-1", endpoint)
	if err != nil {
		t.Fatalf("events adapter: %v", err)
	}
	dlq, err := NewSQSAdapter(ctx, account+"wager-events-dlq", "us-east-1", endpoint)
	if err != nil {
		t.Fatalf("dlq adapter: %v", err)
	}

	marker := `{"eventId":"topology-` + time.Now().UTC().Format("150405.000000000") + `"}`
	for name, a := range map[string]*SQSAdapter{"events": events, "dlq": dlq} {
		body := strings.Replace(marker, "topology", "topology-"+name, 1)
		if err := a.SendMessage(ctx, []byte(body)); err != nil {
			t.Fatalf("send %s: %v", name, err)
		}
		if got := receiveBody(t, ctx, a, body); got != body {
			t.Errorf("%s round trip = %q, want byte-identical %q", name, got, body)
		}
	}
}

// receiveBody pesquisa a mensagem própria (comporta leftovers de outras
// execuções, que são descartadas com delete) até o deadline do contexto.
func receiveBody(t *testing.T, ctx context.Context, a *SQSAdapter, want string) string {
	t.Helper()

	cli, ok := a.client.(*awssqs.Client)
	if !ok {
		t.Fatalf("adapter sem cliente real")
	}
	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("message %q never received: %v", want, err)
		}
		out, err := cli.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            &a.queueURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     2,
		})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		for _, m := range out.Messages {
			if m.ReceiptHandle == nil || m.Body == nil {
				continue
			}
			_, _ = cli.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
				QueueUrl:      &a.queueURL,
				ReceiptHandle: m.ReceiptHandle,
			})
			if *m.Body == want {
				return *m.Body
			}
		}
	}
}
