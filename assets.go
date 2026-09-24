// Package northfen embeds the repo's configuration and synthetic demo data so
// every binary (CLI, local services, Lambdas) carries the same copy and no
// Lambda needs files copied next to it.
package northfen

import "embed"

// DefaultConfig is config/northfen.yaml.
//
//go:embed config/northfen.yaml
var DefaultConfig []byte

// Demo holds demo/equipment.yaml, demo/scenarios/*.yaml, demo/expected.json
// and the render-manager exports replayed by some scenarios (demo/deadline).
//
//go:embed demo/equipment.yaml demo/scenarios/*.yaml demo/expected.json demo/deadline/*.json
var Demo embed.FS
