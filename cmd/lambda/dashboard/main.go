// Command dashboard is the hosted live console (behind marian.online's
// /demos/northfen rewrite). Password from SSM; every sign-in gets a private
// sandbox; runs stream through the simulator Lambda -> Kinesis.
package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/awsapp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dashboard"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/httplambda"
)

func main() {
	ctx := context.Background()
	app, err := awsapp.New(ctx, awsapp.EnvFromOS())
	if err != nil {
		log.Fatal(err)
	}
	pw, err := app.Secret(app.Env.DashboardPassword).Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	mode := "AWS · Kinesis + Lambda · " + app.Svc.Model.Name()
	srv, err := dashboard.New(dashboard.Options{
		Svc: app.Svc, Starter: app.Starter(), Password: pw, SecureCookie: true, Sandboxes: true,
		BasePath: os.Getenv("NF_DASHBOARD_BASE_PATH"), SiteURL: os.Getenv("NF_DASHBOARD_SITE_URL"),
		AllowedOrigins: strings.Split(os.Getenv("NF_DASHBOARD_ALLOWED_ORIGINS"), ","), Mode: mode, Log: app.Log,
	})
	if err != nil {
		log.Fatal(err)
	}
	lambda.Start(httplambda.Handler(srv.Handler()))
}
