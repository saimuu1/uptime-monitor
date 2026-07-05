# Roadmap — elevating the Uptime Monitor

**Where it stands:** v1–v4 complete (single process → NATS worker split →
multi-region consensus + alerts + status page → containers, CI, Terraform,
Prometheus/Grafana), plus email alerts and a self-serve add/remove web UI.
It runs locally and works end to end.

**Goal:** make it always-on, feature-rich, a real multi-user product, and
portfolio-ready — all without spending money.

Do the phases roughly in order: Phase 0 → 1 give the biggest payoff for the least
work. Phase 3 is the big one. Phase 4 runs alongside everything.

---

## Phase 0 — Lock the front door (do BEFORE any public deploy) · small

The add/remove form has **no login**. On the public internet, anyone could add
sites or trigger emails from your account. Before exposing it, add a simple gate.

- Add HTTP Basic Auth middleware to the web server (one shared user/password from
  env `WEB_USER` / `WEB_PASS`); skip it when unset so local dev stays open.
- Files: `cmd/web/main.go` (wrap the mux), `internal/env` (the two vars).
- This is a stopgap until real accounts (Phase 3).

---

## Phase 1 — Get it live 24/7, for free · small–medium

Pick **one** host and get a public, always-on instance:

- **A) GitHub Student Pack → DigitalOcean** — if you're a student, the pack gives
  ~$200 DO credit. Then the existing `deploy/terraform` `terraform apply` runs the
  real multi-region setup for free. Best option if you qualify.
  ([education.github.com/pack](https://education.github.com/pack))
- **B) Oracle Cloud "Always Free" VM** — free-forever VM (card for verification
  only, no charge). Install Docker, `docker compose up -d`. One always-on box.
- **C) Spare laptop / Raspberry Pi at home** — `docker compose up -d`, leave it on.
  Truly free; not "the cloud" but genuinely 24/7.

Then:
- Put HTTPS in front (Caddy or a free Cloudflare Tunnel — both free).
- Point it at your real sites, turn on email (`deploy/.env`).
- **Outcome: a live demo URL** — the single biggest résumé upgrade here.

---

## Phase 2 — Standout features · medium (pick 2–3)

Each is self-contained and builds on existing code. Add tests with each.

1. **SSL certificate expiry check** — for HTTPS monitors, read the cert's
   `NotAfter` and warn when it's within N days of expiring. New alert reason.
   *(Real problem sites hit; very impressive.)* — `internal/check`, `internal/alert`.
2. **Keyword check** — a monitor is "up" only if the page body contains expected
   text. Catches "returns 200 but the page is broken." — schema column +
   `internal/check` + a form field.
3. **Uptime history bars** — the 90-day green/red bar strip real status pages
   show. You already store every check. — `internal/store` query + `cmd/web`.
4. **Incident history page** — `/incidents` listing past outages and how long each
   lasted. — `internal/store` + `cmd/web`.
5. **Slow-response alerts** — alert when a site is *slow* (latency over a
   threshold for a while), not just fully down. Reuse the flap-suppression engine.

---

## Phase 3 — The product leap: user accounts · large

Turn "one shared page" into "everyone manages their own sites." This is the
biggest full-stack chunk and the most interview-valuable.

- **Auth:** GitHub OAuth (less code, no password storage) or sessions + password.
- **Data:** a `users` table; add `user_id` to `monitors`; scope every query to the
  logged-in user.
- **UI:** signup/login pages, a "my monitors" dashboard, per-user email default.
- Replace the Phase 0 basic-auth gate.
- **Security:** input validation, rate-limit the add endpoint, CSRF on forms.

---

## Phase 4 — Portfolio polish · small, ongoing (highest job-hunt ROI)

- README: **screenshots + a short GIF** of an outage → email, the architecture
  diagram, the **live demo link**, and a "what I learned" section.
- A short **blog post / case study** on building it.
- Résumé bullet, e.g.: *"Built a distributed, multi-region uptime monitor in Go
  (NATS, TimescaleDB, Docker, Terraform, Prometheus/Grafana) with consensus-based
  alerting, email notifications, and a self-serve web UI; deployed live."*

---

## Suggested schedule

| When | Focus |
|---|---|
| Week 1 | Phase 0 (auth gate) + Phase 1 (get it live) + start Phase 4 (screenshots) |
| Weeks 2–3 | Phase 2 — ship 2–3 features |
| Weeks 4–6 | Phase 3 — user accounts |
| Ongoing | Phase 4 — write-up, demo GIF, résumé |

**Start here:** Phase 0 is an hour of work and unblocks a safe public deploy in
Phase 1. Ping me for any phase and we'll build it together.
