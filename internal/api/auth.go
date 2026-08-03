package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
)

// sessionCookie names the cookie exchanged for the launch token.
const sessionCookie = "dmtx_session"

// newToken returns a hex-encoded cryptographically random secret.
//
// The token exists so that binding to loopback is not, by itself, the
// authorization boundary. Any web page the operator visits can issue requests
// to 127.0.0.1, so a server that trusts everything reaching the port trusts
// every site the operator browses. Since the token is generated at startup and
// carried in the URL the browser is opened at, the operator never sees or types
// it: the access is one click and still authenticated.
func newToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// authenticator holds the launch token and decides whether a request carries
// it.
type authenticator struct {
	token string
}

// grant exchanges a correct launch token for a session cookie and redirects to
// the console.
//
// Redirecting matters beyond tidiness: it removes the token from the address
// bar, so it does not sit in browser history or get copied out of a screenshot
// when an operator shares one.
func (auth *authenticator) grant(writer http.ResponseWriter, request *http.Request) {
	supplied := request.URL.Query().Get("token")
	if !auth.matches(supplied) {
		http.Error(writer, "invalid or missing token", http.StatusUnauthorized)
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name:     sessionCookie,
		Value:    auth.token,
		Path:     "/",
		HttpOnly: true,
		// Strict rather than Lax: no cross-site navigation should ever arrive
		// carrying this session, because every route behind it can start or
		// abandon a migration.
		SameSite: http.SameSiteStrictMode,
		// Not Secure: the server is loopback-only plaintext by design, and a
		// Secure cookie would simply never be sent.
	})
	http.Redirect(writer, request, "/", http.StatusFound)
}

// matches compares in constant time. A token check that returns early on the
// first differing byte leaks its position to anything that can time it.
func (auth *authenticator) matches(supplied string) bool {
	if supplied == "" || auth.token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(supplied), []byte(auth.token)) == 1
}

// require wraps a handler so it only runs for an authenticated request.
//
// A bearer header is accepted alongside the cookie so scripts and the CLI's own
// parity tests can call the API without pretending to be a browser.
func (auth *authenticator) require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if cookie, err := request.Cookie(sessionCookie); err == nil &&
			auth.matches(cookie.Value) {
			next.ServeHTTP(writer, request)
			return
		}
		header := request.Header.Get("Authorization")
		if supplied, found := strings.CutPrefix(header, "Bearer "); found &&
			auth.matches(supplied) {
			next.ServeHTTP(writer, request)
			return
		}
		http.Error(writer, "authentication required", http.StatusUnauthorized)
	})
}
