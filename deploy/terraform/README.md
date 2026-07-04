# Deploy to DigitalOcean (v4)

Stands up the whole system in the cloud: one **core** droplet (database, NATS
queue, scheduler, evaluator, status page) and one **checker** droplet per region.
Each checker is tagged with its region, so the evaluator's consensus is now real
— an outage must be seen from multiple continents before it alerts.

```
             ┌─ uptime-core (nyc1) ─────────────┐
             │ db · nats · scheduler ·          │
             │ evaluator · web (:8090)          │
             └────────────▲──────────▲──────────┘
                  nats:4222 │          │ nats:4222
         ┌──────────────────┘          └───────────────────┐
   uptime-checker-lon1                              uptime-checker-sgp1
```

> ⚠️ **This costs money.** Three `s-1vcpu-1gb` droplets are about **$18/month**
> total (~$6 each), billed hourly — so a short test is cents. Everything up to
> `terraform apply` is free.

## One-time setup

1. **DigitalOcean account** + an API token: cloud.digitalocean.com/account/api
2. **An SSH key uploaded to DO** (so you can log into the droplets). Get its id:
   `doctl compute ssh-key list`.
3. **Push the images** to a registry the droplets can pull from. The
   `Release images` GitHub Action pushes to GHCR when you tag a release:
   ```bash
   git tag v0.4.0 && git push --tags
   ```
   Then make the GHCR packages public (or add a pull secret). By default the
   Terraform pulls from `ghcr.io/saimuu1/uptime-*`.

## Deploy

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in token, ssh key, regions
terraform init
terraform plan       # preview: shows every server it will create
terraform apply      # create it (costs start here)
```

`apply` prints the `status_url` — give the droplet a minute to boot, install
Docker, and pull images, then open it. Take a monitored site down and watch every
region agree before it alerts.

## Tear it down (stop paying)

```bash
terraform destroy
```

## Notes / production hardening

- The core box runs Postgres/TimescaleDB in a container with a volume. For real
  use, swap in DO Managed Postgres (note: the timescaledb extension may not be
  available there — the `checks` table would become a plain table).
- NATS is exposed on 4222 with no auth, locked to the checker IPs by the
  firewall. For production, add NATS auth + TLS.
- State is local (`terraform.tfstate`, gitignored). For a team, use a remote
  backend (DO Spaces).
