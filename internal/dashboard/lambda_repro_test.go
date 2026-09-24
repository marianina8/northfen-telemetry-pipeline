package dashboard_test

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dashboard"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/httplambda"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store/storetest"
)

func TestLambdaShapedRequests(t *testing.T) {
	fake := storetest.NewFakeDynamo("r", "s", "a")
	svc := &pipeline.Service{Cfg: config.Default(), Cat: sim.MustCatalog(), Model: &explain.Mock{}, ExpireAll: true,
		Sink: &dispatch.PagerSink{Next: &dispatch.LogSink{}}, Store: &store.Dynamo{Client: fake, ReadingsTable: "r", StateTable: "s", AppTable: "a"}}
	srv, err := dashboard.New(dashboard.Options{Svc: svc, Starter: &syncStarter{svc: svc}, Password: "pw", SecureCookie: true, Sandboxes: true,
		BasePath: "/demos/northfen", SiteURL: "/demos", AllowedOrigins: []string{"https://marian.online", "https://www.marian.online"}, Mode: "AWS"})
	if err != nil {
		t.Fatal(err)
	}
	h := httplambda.Handler(srv.Handler())
	for _, p := range []string{"/demos/northfen", "/demos/northfen/", "/demos/northfen/login", "/demos/northfen/static/app.css", "/demos/northfen/healthz"} {
		ev := events.APIGatewayV2HTTPRequest{RawPath: p, Headers: map[string]string{"host": "parv32r9oe.execute-api.us-west-2.amazonaws.com"}}
		ev.RequestContext.HTTP.Method = "GET"
		resp, err := h(context.Background(), ev)
		t.Logf("%s -> %d %v err=%v body=%.80q", p, resp.StatusCode, resp.Headers["Location"], err, resp.Body)
		if err != nil || resp.StatusCode >= 500 {
			t.Fatalf("%s failed", p)
		}
	}
}
