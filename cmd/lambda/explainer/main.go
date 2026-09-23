// Command explainer is the SQS explain worker Lambda: one bounded Bedrock
// call per flagged alert, then the deterministic dispatch table.
package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/awsapp"
)

func main() {
	app, err := awsapp.New(context.Background(), awsapp.EnvFromOS())
	if err != nil {
		log.Fatal(err)
	}
	x := &awsapp.Explainer{Svc: app.Svc, Queue: app.Svc.Queue, Log: app.Log}
	lambda.Start(x.Handle)
}
