// Package httplambda runs a standard net/http handler behind API Gateway
// HTTP APIs (payload format 2.0), so the same dashboard code serves locally
// and on Lambda. It is intentionally tiny: request in, recorded response out.
package httplambda

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"
)

// Handler adapts h to an API Gateway v2 Lambda handler.
func Handler(h http.Handler) func(context.Context, events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	return func(ctx context.Context, ev events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
		r, err := ToRequest(ctx, ev)
		if err != nil {
			return events.APIGatewayV2HTTPResponse{StatusCode: http.StatusBadRequest, Body: "bad request"}, nil
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return FromRecorder(rec), nil
	}
}

// ToRequest converts an API Gateway v2 event into an *http.Request.
func ToRequest(ctx context.Context, ev events.APIGatewayV2HTTPRequest) (*http.Request, error) {
	body := []byte(ev.Body)
	if ev.IsBase64Encoded {
		b, err := base64.StdEncoding.DecodeString(ev.Body)
		if err != nil {
			return nil, err
		}
		body = b
	}
	path := ev.RawPath
	if path == "" {
		path = "/"
	}
	u := &url.URL{Path: path, RawQuery: ev.RawQueryString}
	method := ev.RequestContext.HTTP.Method
	if method == "" {
		method = http.MethodGet
	}
	r, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range ev.Headers {
		r.Header.Set(k, v) // v2 joins repeated headers with commas already
	}
	if len(ev.Cookies) > 0 {
		r.Header.Set("Cookie", strings.Join(ev.Cookies, "; "))
	}
	r.Host = ev.Headers["host"]
	if r.Host == "" {
		r.Host = ev.RequestContext.DomainName
	}
	r.RemoteAddr = ev.RequestContext.HTTP.SourceIP
	r.RequestURI = u.RequestURI()
	return r, nil
}

// FromRecorder converts a recorded response into an API Gateway v2 response.
func FromRecorder(rec *httptest.ResponseRecorder) events.APIGatewayV2HTTPResponse {
	res := rec.Result()
	out := events.APIGatewayV2HTTPResponse{StatusCode: res.StatusCode, Headers: map[string]string{}}
	for k, vs := range res.Header {
		if strings.EqualFold(k, "Set-Cookie") {
			out.Cookies = append(out.Cookies, vs...)
			continue
		}
		out.Headers[k] = strings.Join(vs, ", ")
	}
	b := rec.Body.Bytes()
	if out.Headers["Content-Type"] == "" && len(b) > 0 {
		// Belt and braces: net/http sniffs a missing Content-Type, the
		// recorder doesn't, and API Gateway would otherwise send none.
		out.Headers["Content-Type"] = http.DetectContentType(b)
	}
	if utf8.Valid(b) {
		out.Body = string(b)
	} else {
		out.Body, out.IsBase64Encoded = base64.StdEncoding.EncodeToString(b), true
	}
	return out
}
