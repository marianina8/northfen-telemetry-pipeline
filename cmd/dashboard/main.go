// Command dashboard runs the live anomaly console locally
// (http://127.0.0.1:8080). Scenarios stream in-process through the same
// pipeline code the Lambdas run; data lives in the CLI's -data directory, so
// `northfen feed` / `northfen ack` see the same alerts.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/awsapp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dashboard"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/localapp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	data := flag.String("data", ".northfen", "local data directory (shared with the CLI)")
	bedrock := flag.Bool("bedrock", false, "use Amazon Bedrock for the explain step (default: offline mock)")
	profile := flag.String("profile", "demos-admin", "AWS profile for Bedrock")
	password := flag.String("password", os.Getenv("NF_DASHBOARD_PASSWORD"), "require this password (and give every sign-in a private sandbox)")
	base := flag.String("base", "", "mount under this path, e.g. /demos/northfen")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	app, err := localapp.Open(ctx, localapp.Options{DataDir: *data, Bedrock: *bedrock, Profile: *profile})
	if err != nil {
		fmt.Fprintln(os.Stderr, "dashboard:", err)
		os.Exit(1)
	}
	defer app.Close()
	starter := &awsapp.LocalStarter{Svc: app.Svc, Ctx: ctx}
	mode := "Local · offline mock explainer"
	if *bedrock {
		mode = "Local · Amazon Bedrock"
	}
	srv, err := dashboard.New(dashboard.Options{Svc: app.Svc, Starter: starter, Password: *password, Sandboxes: *password != "",
		BasePath: *base, Mode: mode, Log: slog.Default()})
	if err != nil {
		fmt.Fprintln(os.Stderr, "dashboard:", err)
		os.Exit(1)
	}
	hs := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		hs.Shutdown(sctx)
	}()
	fmt.Printf("Northfen console on http://%s%s/  (data: %s, %s)\n", *addr, *base, *data, mode)
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "dashboard:", err)
		os.Exit(1)
	}
}
