// Package embedding implements text embedding providers for semantic indexing.
package embedding

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/option"
	"goodkind.io/claude-context-go/internal/config"
)

const maxEmbeddingTokens = 8192

// Provider generates dense embedding vectors.
type Provider interface {
	Embed(context.Context, string) ([]float32, error)
	EmbedBatch(context.Context, []string) ([][]float32, error)
	ProviderName() string
}

// NewProvider constructs the configured embedding provider.
//
// Only the OpenAI-compatible HTTP adapter is supported. Users point it at any
// upstream that speaks the OpenAI embeddings API by setting OPENAI_BASE_URL
// (for example a self-hosted Ollama with `/v1/embeddings`, an OpenRouter
// account, or the OpenAI service itself).
func NewProvider(cfg config.Config) (Provider, error) {
	provider := strings.TrimSpace(cfg.EmbeddingProvider)
	if provider != "" && !strings.EqualFold(provider, "OpenAI") {
		slog.Error("embedding provider is not supported", "provider", provider, "err", errors.New("only OpenAI-compatible adapter is supported"))
		return nil, fmt.Errorf("embedding provider %q is not supported; only the OpenAI-compatible adapter is available", provider)
	}
	return newOpenAICompatibleProvider("OpenAI", cfg.OpenAIAPIKey, cfg.OpenAIBaseURL, cfg.EmbeddingModel, cfg.EmbeddingDimension)
}

type openAICompatibleProvider struct {
	name       string
	model      string
	dimensions int32
	client     openai.Client
}

func newOpenAICompatibleProvider(name string, apiKey string, baseURL string, model string, dimensions int32) (Provider, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("%s embedding provider requires an API key", name)
	}
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("%s embedding provider requires a model", name)
	}

	requestOptions := []option.RequestOption{option.WithAPIKey(apiKey)}
	if strings.TrimSpace(baseURL) != "" {
		requestOptions = append(requestOptions, option.WithBaseURL(baseURL))
	}

	return &openAICompatibleProvider{
		name:       name,
		model:      model,
		dimensions: dimensions,
		client:     openai.NewClient(requestOptions...),
	}, nil
}

func (provider *openAICompatibleProvider) ProviderName() string {
	return provider.name
}

func (provider *openAICompatibleProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	embeddings, err := provider.EmbedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(embeddings) == 0 {
		return nil, fmt.Errorf("%s embedding provider returned no vectors", provider.name)
	}
	return embeddings[0], nil
}

func (provider *openAICompatibleProvider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	preprocessedTexts := make([]string, 0, len(texts))
	for _, text := range texts {
		preprocessedTexts = append(preprocessedTexts, preprocessText(text))
	}

	params := openai.EmbeddingNewParams{
		Input: openai.EmbeddingNewParamsInputUnion{
			OfArrayOfStrings: preprocessedTexts,
		},
		Model:          provider.model,
		EncodingFormat: openai.EmbeddingNewParamsEncodingFormatFloat,
	}
	if provider.dimensions > 0 {
		params.Dimensions = openai.Int(int64(provider.dimensions))
	}

	response, err := provider.client.Embeddings.New(ctx, params)
	if err != nil {
		slog.ErrorContext(ctx, "generate embeddings failed", "provider", provider.name, "model", provider.model, "err", err)
		return nil, fmt.Errorf("generate %s embeddings: %w", provider.name, err)
	}
	if len(response.Data) != len(preprocessedTexts) {
		slog.ErrorContext(ctx, "embedding provider returned unexpected vector count", "provider", provider.name, "want", len(preprocessedTexts), "got", len(response.Data), "err", errors.New("vector count mismatch"))
		return nil, fmt.Errorf("%s embedding provider returned %d vectors for %d texts", provider.name, len(response.Data), len(preprocessedTexts))
	}

	vectors := make([][]float32, 0, len(response.Data))
	for _, item := range response.Data {
		vector := make([]float32, 0, len(item.Embedding))
		for _, value := range item.Embedding {
			vector = append(vector, float32(value))
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

func preprocessText(text string) string {
	if text == "" {
		return " "
	}

	maxCharacters := maxEmbeddingTokens * 4
	if len(text) > maxCharacters {
		return text[:maxCharacters]
	}
	return text
}
