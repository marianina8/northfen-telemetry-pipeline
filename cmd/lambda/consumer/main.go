// Command consumer is the Kinesis stream consumer Lambda: windowed,
// deterministic scoring only. It never calls a model.
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
	c := &awsapp.Consumer{Svc: app.Svc, Log: app.Log}
	lambda.Start(c.Handle)
}
