package httplambda

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestRoundTrip(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/submit" || r.URL.Query().Get("a") != "1" {
			t.Errorf("request = %s %s", r.Method, r.URL)
		}
		if c, err := r.Cookie("icr_session"); err != nil || c.Value != "tok" {
			t.Errorf("cookie not forwarded: %v", err)
		}
		if r.Host != "abc.execute-api.us-west-2.amazonaws.com" {
			t.Errorf("host = %s", r.Host)
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != "x=y" {
			t.Errorf("body = %q", b)
		}
		http.SetCookie(w, &http.Cookie{Name: "a", Value: "1"})
		http.SetCookie(w, &http.Cookie{Name: "b", Value: "2"})
		w.Header().Set("Location", "/items/AL-1")
		w.WriteHeader(http.StatusSeeOther)
	})
	ev := events.APIGatewayV2HTTPRequest{
		RawPath: "/submit", RawQueryString: "a=1",
		Headers: map[string]string{"host": "abc.execute-api.us-west-2.amazonaws.com", "content-type": "application/x-www-form-urlencoded"},
		Cookies: []string{"icr_session=tok", "other=z"},
		Body:    base64.StdEncoding.EncodeToString([]byte("x=y")), IsBase64Encoded: true,
	}
	ev.RequestContext.HTTP.Method = "POST"
	resp, err := Handler(h)(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 303 || resp.Headers["Location"] != "/items/AL-1" || len(resp.Cookies) != 2 {
		t.Errorf("resp = %+v", resp)
	}
	if _, ok := resp.Headers["Set-Cookie"]; ok {
		t.Error("Set-Cookie must go in Cookies, not Headers")
	}
}
