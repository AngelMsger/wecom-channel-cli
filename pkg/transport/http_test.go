package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNoRedirectOrEnvironmentProxy(t *testing.T) {
	hits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(200) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"?access_token=SECRET", http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	t.Setenv("HTTP_PROXY", target.URL)
	t.Setenv("HTTPS_PROXY", target.URL)
	t.Setenv("ALL_PROXY", target.URL)
	r, _ := http.NewRequest("POST", source.URL+"?access_token=SECRET", nil)
	resp, err := New().Do(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if hits != 0 || resp.StatusCode != 307 {
		t.Fatal("redirect or proxy followed")
	}
}
