// Command simulator streams a recorded run's synthetic readings into
// Kinesis at the run's pace (invoked asynchronously by the dashboard and the
// simulate webhook).
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
	sm := &awsapp.Simulator{Svc: app.Svc, Producer: app.Producer(), Log: app.Log}
	lambda.Start(sm.Handle)
}
