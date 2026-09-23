package dashboard

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
)

const (
	sessionCookie = "nf_session"
	sessionTTL    = 12 * time.Hour
)

// auth is a single shared-password login for demo access. Sessions are
// stateless HMAC-signed cookies ("<expiry>.<sig>"), so they work across
// Lambda instances; the signing key is derived from the password, so
// changing the password logs everyone out.
type auth struct {
	password []byte
	key      []byte
	secure   bool
	path     string // cookie path (the dashboard's base path)
	now      func() time.Time
}

func newAuth(password string, secure bool) *auth {
	k := sha256.Sum256([]byte("northfen-telemetry-dashboard-session\x00" + password))
	return &auth{password: []byte(password), key: k[:], secure: secure, path: "/", now: time.Now}
}

// Tokens are "<expiry>.<workspace>.<sig>"; the signature covers both, so a
// visitor can't switch themselves into someone else's sandbox.
func (a *auth) sign(exp int64, ws string) string {
	m := hmac.New(sha256.New, a.key)
	m.Write([]byte("v2|" + strconv.FormatInt(exp, 10) + "|" + ws))
	return hex.EncodeToString(m.Sum(nil))
}

func (a *auth) token(ws string) (string, time.Time) {
	exp := a.now().Add(sessionTTL)
	return strconv.FormatInt(exp.Unix(), 10) + "." + ws + "." + a.sign(exp.Unix(), ws), exp
}

// parse validates a token and returns its workspace.
func (a *auth) parse(tok string) (string, bool) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return "", false
	}
	exp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || a.now().Unix() > exp || !validWorkspace(parts[1]) {
		return "", false
	}
	if !hmac.Equal([]byte(parts[2]), []byte(a.sign(exp, parts[1]))) {
		return "", false
	}
	return parts[1], true
}

func (a *auth) valid(tok string) bool { _, ok := a.parse(tok); return ok }

// validWorkspace accepts "" (shared, when sandboxes are off) or a sandbox
// ID: "v" + 16 hex characters (what pipeline.IsSandbox recognizes).
func validWorkspace(ws string) bool { return ws == "" || pipeline.IsSandbox(ws) }

// newWorkspaceID returns a random sandbox ID (hex).
func newWorkspaceID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "v" + hex.EncodeToString(b)
}

type ctxKey struct{}

// workspace returns the sandbox for this request ("" = shared).
func workspace(r *http.Request) string {
	ws, _ := r.Context().Value(ctxKey{}).(string)
	return ws
}

func (a *auth) check(password string) bool {
	// Compare fixed-size digests so timing doesn't leak the length.
	x, y := sha256.Sum256([]byte(password)), sha256.Sum256(a.password)
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}

func (a *auth) session(r *http.Request) (string, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return "", false
	}
	return a.parse(c.Value)
}

func (a *auth) authed(r *http.Request) bool { _, ok := a.session(r); return ok }

// requireAuth redirects unauthenticated requests to the login page.
func (s *Server) requireAuth(h http.Handler) http.Handler {
	if s.auth == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" || r.URL.Path == "/healthz" || strings.HasPrefix(r.URL.Path, "/static/") {
			h.ServeHTTP(w, r)
			return
		}
		if ws, ok := s.auth.session(r); ok {
			h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, ws)))
			return
		}
		if r.Method != http.MethodGet || strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"sign in required"}`))
			return
		}
		http.Redirect(w, r, s.url("/login?next=")+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
	})
}

type loginData struct {
	Page  page
	Next  string
	Error string
}

// safeNext only allows local paths, so the login form can't be used as an
// open redirect.
func safeNext(n string) string {
	if !strings.HasPrefix(n, "/") || strings.HasPrefix(n, "//") || strings.HasPrefix(n, "/\\") {
		return "/"
	}
	return n
}

func (s *Server) loginForm(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil || s.auth.authed(r) {
		http.Redirect(w, r, s.url("/"), http.StatusSeeOther)
		return
	}
	p := s.page("Sign in")
	p.Nav = false
	s.render(w, "login.html", loginData{Page: p, Next: safeNext(r.URL.Query().Get("next"))})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		http.Redirect(w, r, s.url("/"), http.StatusSeeOther)
		return
	}
	next := safeNext(r.FormValue("next"))
	if !s.auth.check(r.FormValue("password")) {
		time.Sleep(400 * time.Millisecond) // slow down guessing
		p := s.page("Sign in")
		p.Nav = false
		s.renderStatus(w, http.StatusUnauthorized, "login.html", loginData{Page: p, Next: next, Error: "That password isn't right."})
		return
	}
	ws := ""
	if s.opt.Sandboxes {
		// Every sign-in gets a fresh, empty private sandbox.
		ws = newWorkspaceID()
	}
	tok, exp := s.auth.token(ws)
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: tok, Path: s.auth.path, Expires: exp,
		HttpOnly: true, Secure: s.auth.secure, SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, s.url(next), http.StatusSeeOther)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	secure, path := false, "/"
	if s.auth != nil {
		secure, path = s.auth.secure, s.auth.path
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: path, MaxAge: -1, HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, s.url("/login"), http.StatusSeeOther)
}
