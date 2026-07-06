package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/saimuu1/uptime-monitor/internal/store"
)

const (
	sessionCookie = "session"
	sessionTTL    = 30 * 24 * time.Hour
)

type ctxKey int

const userKey ctxKey = 0

// auth bundles the store and templates for the login/signup handlers.
type auth struct {
	st   *store.Store
	tmpl *template.Template
}

// userID returns the logged-in user's id from the request context (0 if none).
func userID(ctx context.Context) int64 {
	id, _ := ctx.Value(userKey).(int64)
	return id
}

// gate is middleware: public paths pass through; everything else needs a valid
// session, else it redirects to /login. The user id is put on the context.
func (a *auth) gate(next http.Handler) http.Handler {
	public := map[string]bool{"/login": true, "/signup": true, "/logout": true, "/healthz": true, "/metrics": true}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if public[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		uid, ok := a.userFromRequest(r)
		if !ok {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, uid)))
	})
}

func (a *auth) userFromRequest(r *http.Request) (int64, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return 0, false
	}
	uid, ok, err := a.st.UserBySession(r.Context(), c.Value)
	if err != nil {
		log.Printf("session lookup: %v", err)
		return 0, false
	}
	return uid, ok
}

func (a *auth) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) { a.render(w, "login.html", "") })
	mux.HandleFunc("GET /signup", func(w http.ResponseWriter, r *http.Request) { a.render(w, "signup.html", "") })
	mux.HandleFunc("POST /signup", a.signup)
	mux.HandleFunc("POST /login", a.login)
	mux.HandleFunc("POST /logout", a.logout)
}

func (a *auth) signup(w http.ResponseWriter, r *http.Request) {
	email := strings.TrimSpace(strings.ToLower(r.FormValue("email")))
	pass := r.FormValue("password")
	if email == "" || len(pass) < 8 {
		a.render(w, "signup.html", "Enter an email and a password of at least 8 characters.")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	uid, err := a.st.CreateUser(r.Context(), email, string(hash))
	if errors.Is(err, store.ErrEmailTaken) {
		a.render(w, "signup.html", "That email is already registered — try logging in.")
		return
	}
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	a.startSession(w, r, uid)
}

func (a *auth) login(w http.ResponseWriter, r *http.Request) {
	email := strings.TrimSpace(strings.ToLower(r.FormValue("email")))
	pass := r.FormValue("password")
	id, hash, ok, err := a.st.UserByEmail(r.Context(), email)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if !ok || bcrypt.CompareHashAndPassword([]byte(hash), []byte(pass)) != nil {
		a.render(w, "login.html", "Wrong email or password.")
		return
	}
	a.startSession(w, r, id)
}

func (a *auth) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		_ = a.st.DeleteSession(r.Context(), c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// startSession creates a session, sets the cookie, and redirects to the app.
func (a *auth) startSession(w http.ResponseWriter, r *http.Request, uid int64) {
	tok := randomToken()
	if err := a.st.CreateSession(r.Context(), tok, uid, time.Now().Add(sessionTTL)); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: tok, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, Expires: time.Now().Add(sessionTTL),
	})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (a *auth) render(w http.ResponseWriter, page, errMsg string) {
	if err := a.tmpl.ExecuteTemplate(w, page, map[string]string{"Error": errMsg}); err != nil {
		log.Printf("render %s: %v", page, err)
	}
}

func randomToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
