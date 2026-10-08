# Build da API (README §4: Go com versão declarada aqui e no go.mod).
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/server ./cmd/server

# Runtime mínimo: binário estático, usuário sem privilégio, ca-certs para
# JWKS/discovery OIDC sobre HTTPS.
FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget && \
    adduser -D -H -u 10001 appuser
COPY --from=build /out/server /usr/local/bin/server
USER appuser
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/server"]
