// Package observability concentra a operabilidade mínima do serviço
// (B3.14): correlation/request ID, logging estruturado, métricas e health.
// Camadas de infra/transporte/application podem depender daqui; o domínio
// (internal/domain) nunca deve importar este pacote. Sem dependências
// externas: stdlib apenas (log/slog, net/http, sync).
package observability
