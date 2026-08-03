package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
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

// authenticator holds the launch token and the session secret it is exchanged
// for.
//
// They are separate values, and the launch token really is single-use: it is
// cleared the first time it is redeemed. Reusing one secret for both would make
// the URL a long-lived bearer credential, so anywhere that URL came to rest -
// a shell history, a pasted message, a screenshot - would stay usable for as
// long as the server ran. Describing it as one-time while accepting it forever
// is the kind of claim this codebase keeps finding in its own tests.
type authenticator struct {
	mutex   sync.Mutex
	launch  string
	session string
}

// grant exchanges a correct launch token for a session cookie and redirects to
// the console.
//
// Redirecting matters beyond tidiness: it removes the token from the address
// bar, so it does not sit in browser history or get copied out of a screenshot
// when an operator shares one.
func (auth *authenticator) grant(writer http.ResponseWriter, request *http.Request) {
	supplied := request.URL.Query().Get("token")
	if !auth.redeem(supplied) {
		http.Error(writer, "invalid or missing token", http.StatusUnauthorized)
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name:     sessionCookie,
		Value:    auth.session,
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

// redeem consumes the launch token. It succeeds at most once: the second
// caller, whoever they are, finds nothing to redeem.
func (auth *authenticator) redeem(supplied string) bool {
	auth.mutex.Lock()
	defer auth.mutex.Unlock()
	if !constantTimeEqual(supplied, auth.launch) {
		return false
	}
	auth.launch = ""
	return true
}

// remint issues a replacement launch token, so a second invocation can be sent
// to this server with a URL that is single-use like the original.
//
// It replaces rather than adds: at most one launch token is outstanding, so two
// handoffs racing leave only the later one usable. The operator whose browser
// arrives with the older token is told the token is invalid and runs the
// command again, which is a worse morning than a queue of valid tokens would
// give them but a much better one than a URL that stays live.
func (auth *authenticator) remint() (string, error) {
	token, err := newToken()
	if err != nil {
		return "", err
	}
	auth.mutex.Lock()
	defer auth.mutex.Unlock()
	auth.launch = token
	return token, nil
}

// holdsSession reports whether a value is the session secret.
func (auth *authenticator) holdsSession(supplied string) bool {
	return constantTimeEqual(supplied, auth.session)
}

// constantTimeEqual compares without returning early on the first differing
// byte, which would leak its position to anything that can time the call.
func constantTimeEqual(supplied, expected string) bool {
	if supplied == "" || expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(supplied), []byte(expected)) == 1
}

// require wraps a handler so it only runs for an authenticated request.
//
// A bearer header is accepted alongside the cookie so scripts and the CLI's own
// parity tests can call the API without pretending to be a browser.
func (auth *authenticator) require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if cookie, err := request.Cookie(sessionCookie); err == nil &&
			auth.holdsSession(cookie.Value) {
			next.ServeHTTP(writer, request)
			return
		}
		header := request.Header.Get("Authorization")
		if supplied, found := strings.CutPrefix(header, "Bearer "); found &&
			auth.holdsSession(supplied) {
			next.ServeHTTP(writer, request)
			return
		}
		http.Error(writer, "authentication required", http.StatusUnauthorized)
	})
}
