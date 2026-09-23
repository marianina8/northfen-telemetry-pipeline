package explain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
)

// ConverseAPI is the one Bedrock Runtime call used (fakeable in tests).
type ConverseAPI interface {
	Converse(ctx context.Context, in *bedrockruntime.ConverseInput, opts ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error)
}

// Bedrock explains a flagged window with one Converse call.
type Bedrock struct {
	client     ConverseAPI
	cfg        config.Bedrock
	categories []string
	now        func() time.Time
}

// NewBedrock wires the explainer around a Converse client.
func NewBedrock(client ConverseAPI, c *config.Config) (*Bedrock, error) {
	if c.Explain.Bedrock.ModelID == "" {
		return nil, fmt.Errorf("explain.bedrock.model_id is required")
	}
	return &Bedrock{client: client, cfg: c.Explain.Bedrock, categories: c.Explain.CauseCategories, now: time.Now}, nil
}

// NewBedrockFromEnv uses the default AWS credential chain (env, profile,
// Lambda role). Loading config makes no network call.
func NewBedrockFromEnv(ctx context.Context, c *config.Config, profile string) (*Bedrock, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(c.Explain.Bedrock.Region)}
	if profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return NewBedrock(bedrockruntime.NewFromConfig(awsCfg), c)
}

// Name implements Model.
func (b *Bedrock) Name() string { return "bedrock:" + b.cfg.ModelID }

// Explain implements Model.
func (b *Bedrock) Explain(ctx context.Context, in Input) (Explanation, error) {
	usr, err := UserPrompt(in)
	if err != nil {
		return Explanation{}, err
	}
	maxTokens := b.cfg.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 700
	}
	out, err := b.client.Converse(ctx, &bedrockruntime.ConverseInput{
		ModelId: aws.String(b.cfg.ModelID),
		System:  []types.SystemContentBlock{&types.SystemContentBlockMemberText{Value: SystemPrompt(b.categories)}},
		Messages: []types.Message{{
			Role:    types.ConversationRoleUser,
			Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: usr}},
		}},
		InferenceConfig: &types.InferenceConfiguration{
			MaxTokens:   aws.Int32(maxTokens),
			Temperature: aws.Float32(b.cfg.Temperature),
		},
	})
	if err != nil {
		return Explanation{}, fmt.Errorf("bedrock converse: %w", err)
	}
	msg, ok := out.Output.(*types.ConverseOutputMemberMessage)
	if !ok {
		return Explanation{}, fmt.Errorf("%w: unexpected Converse output %T", ErrBadOutput, out.Output)
	}
	var text strings.Builder
	for _, block := range msg.Value.Content {
		if tb, ok := block.(*types.ContentBlockMemberText); ok {
			text.WriteString(tb.Value)
		}
	}
	e, err := Parse(text.String(), b.categories)
	if err != nil {
		return Explanation{Raw: text.String()}, err
	}
	e.Model, e.At = b.Name(), b.now().UTC()
	return e, nil
}

// New picks the configured provider.
func New(ctx context.Context, c *config.Config, profile string) (Model, error) {
	switch c.Explain.Provider {
	case "bedrock":
		return NewBedrockFromEnv(ctx, c, profile)
	default:
		return &Mock{}, nil
	}
}
