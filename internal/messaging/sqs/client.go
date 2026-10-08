// Adapter SQS real (borda AWS): os tipos do SDK vivem somente aqui. O
// Consumer depende da interface Receiver, testável com fake sem AWS.
package sqs

import (
	"context"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/joaoricardofp/backend-challenge-go/internal/config"
)

// SQSAdapter implementa Receiver (entrada) e Sender (saída) sobre o SDK AWS
// v2. Guarda client e queue URL; nem o Consumer nem o Publisher veem tipos
// do SDK.
type SQSAdapter struct {
	client   sqsAPI
	queueURL string
}

// Nomes lógicos das filas para métricas e logs. Baixa cardinalidade por
// construção: nunca URLs (variam por ambiente) nem IDs de negócio.
const (
	queueWager  = "wager"
	queueEvents = "events"
	queueDLQ    = "dlq"
)

// Sender é a interface própria e mínima para publicação SQS: envia o body
// (o payload JSON persistido na outbox, sem reserialização) para a fila.
// Sem retry, backoff ou DLQ nesta etapa.
type Sender interface {
	SendMessage(ctx context.Context, body []byte) error
}

// sqsAPI é a superfície do SDK usada pelo adapter. Existe só para os testes
// injetarem um fake sem AWS/LocalStack; o SDK real satisfaz a interface.
type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	SendMessage(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
	GetQueueAttributes(ctx context.Context, params *awssqs.GetQueueAttributesInput, optFns ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
}

// QueueAdapters agrupa os adapters por fila compartilhando UM único cliente
// AWS: Wager recebe da fila de entrada (Receiver do consumer), Outbox envia
// para a fila de eventos e OutboxDLQ para a DLQ de eventos (Senders do
// publisher). Adapter nil = fila não configurada = componente desligado.
type QueueAdapters struct {
	Wager     *SQSAdapter
	Outbox    *SQSAdapter
	OutboxDLQ *SQSAdapter
}

// NewQueueAdapters monta os adapters a partir da configuração, carregando a
// config AWS uma única vez e compartilhando o mesmo *awssqs.Client entre as
// filas (o cliente SQS não é vinculado a uma fila: QueueUrl vai por chamada).
// Com SQS desabilitado, retorna vazio sem tocar em credencial AWS. Suporta
// endpoint customizado (LocalStack/testes) via SQS_ENDPOINT. Não faz chamadas
// de rede além do carregamento de config/padrões de credencial do SDK.
func NewQueueAdapters(ctx context.Context, cfg config.SQSConfig) (*QueueAdapters, error) {
	out := &QueueAdapters{}
	if !cfg.Enabled {
		return out, nil
	}
	if cfg.WagerQueueURL == "" && cfg.OutboxQueueURL == "" && cfg.OutboxDLQURL == "" {
		return out, nil
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	var client *awssqs.Client
	if cfg.Endpoint != "" {
		client = awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
			o.BaseEndpoint = &cfg.Endpoint
		})
	} else {
		client = awssqs.NewFromConfig(awsCfg)
	}
	if cfg.WagerQueueURL != "" {
		out.Wager = &SQSAdapter{client: client, queueURL: cfg.WagerQueueURL}
	}
	if cfg.OutboxQueueURL != "" {
		out.Outbox = &SQSAdapter{client: client, queueURL: cfg.OutboxQueueURL}
	}
	if cfg.OutboxDLQURL != "" {
		out.OutboxDLQ = &SQSAdapter{client: client, queueURL: cfg.OutboxDLQURL}
	}
	return out, nil
}

// NewSQSAdapter monta um adapter para uma fila explícita. Mantido para uso
// direto e testes; o wiring Fx usa NewQueueAdapters.
func NewSQSAdapter(ctx context.Context, queueURL, region, endpoint string) (*SQSAdapter, error) {
	if queueURL == "" {
		return nil, fmt.Errorf("sqs queue URL is required")
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	var client *awssqs.Client
	if endpoint != "" {
		client = awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
			o.BaseEndpoint = &endpoint
		})
	} else {
		client = awssqs.NewFromConfig(awsCfg)
	}
	return &SQSAdapter{client: client, queueURL: queueURL}, nil
}

// ReceiveMessage recebe até maxMessages com long polling (wait) e visibility
// timeout por mensagem. Sem heartbeat/extensão automática nesta etapa: se o
// processamento ultrapassar o visibility timeout, a mensagem reaparece e será
// reprocessada sob a autoridade da Inbox (limitação documentada).
func (a *SQSAdapter) ReceiveMessage(
	ctx context.Context,
	maxMessages, waitTimeSeconds, visibilityTimeout int32,
) ([]ReceivedMessage, error) {
	out, err := a.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            &a.queueURL,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     waitTimeSeconds,
		VisibilityTimeout:   visibilityTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("sqs receive: %w", err)
	}
	msgs := make([]ReceivedMessage, 0, len(out.Messages))
	for _, m := range out.Messages {
		if m.ReceiptHandle == nil || m.Body == nil {
			continue
		}
		msgs = append(msgs, ReceivedMessage{
			ReceiptHandle: *m.ReceiptHandle,
			Body:          []byte(*m.Body),
		})
	}
	return msgs, nil
}

// DeleteMessage faz o ack: só é chamada pelo Consumer depois de Inbox
// Complete, nunca antes do processamento terminar.
func (a *SQSAdapter) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := a.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      &a.queueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		return fmt.Errorf("sqs delete: %w", err)
	}
	return nil
}

// SendMessage publica o body (payload JSON da outbox, byte a byte) na fila
// padrão. Sem MessageGroupId/MessageDeduplicationId por decisão explícita:
// são relevantes só para FIFO e o MessageDeduplicationId tem janela mínima
// de 5 minutos — a correção do sistema não depende dele, e sim da identidade
// estável do evento na outbox (at-least-once com dedup posterior).
func (a *SQSAdapter) SendMessage(ctx context.Context, body []byte) error {
	text := string(body)
	_, err := a.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    &a.queueURL,
		MessageBody: &text,
	})
	if err != nil {
		return fmt.Errorf("sqs send: %w", err)
	}
	return nil
}

// CheckQueue verifica a fila de forma barata e somente-leitura para o health
// check: lê um único atributo (ARN), sem enviar, receber ou alterar
// mensagens. Erro indica fila inacessível/inexistente ou SQS fora do ar.
func (a *SQSAdapter) CheckQueue(ctx context.Context) error {
	_, err := a.client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       &a.queueURL,
		AttributeNames: []awstypes.QueueAttributeName{awstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("sqs queue attributes: %w", err)
	}
	return nil
}
