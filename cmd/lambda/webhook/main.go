// Command webhook is the API Gateway entry point for callers that aren't a
// Kinesis producer: POST /simulate and POST /readings (shared-secret header).
package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/awsapp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/httplambda"
)

func main() {
	app, err := awsapp.New(context.Background(), awsapp.EnvFromOS())
	if err != nil {
		log.Fatal(err)
	}
	h := &awsapp.Webhook{Svc: app.Svc, Starter: app.Starter(), Producer: app.Producer(),
		Token: app.Secret(app.Env.WebhookSecret).Get, Log: app.Log}
	lambda.Start(httplambda.Handler(h))
}
