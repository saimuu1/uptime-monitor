# Deploy free & 24/7 on an Oracle Cloud "Always Free" VM

One always-on VM in the cloud, no cost. (A credit card is required at signup for
identity verification only — Always Free resources aren't charged.)

## 1. Create the account
Sign up at [oracle.com/cloud/free](https://www.oracle.com/cloud/free/). Pick a
home region near you. Verify with a card (no charge for Always Free).

## 2. Create the VM
Console → **Compute → Instances → Create instance**:
- **Image:** Canonical Ubuntu (22.04 or 24.04).
- **Shape:** click *Change shape* → **Ampere** → `VM.Standard.A1.Flex`. Set
  **2 OCPU / 12 GB** (Always Free allows up to 4 OCPU / 24 GB of Ampere). Plenty.
  - If you see "out of capacity", try a different Availability Domain or region,
    or retry later — free ARM capacity comes and goes.
- **SSH keys:** upload your public key (or let it generate one and download it).
- Create it. Copy the **public IP address** when it's up.

## 3. Open the web port (two firewalls!)
Oracle blocks ports in **two** places — do both:
1. **Cloud side:** Networking → your VCN → **Security Lists** → default list →
   **Add Ingress Rule**: Source `0.0.0.0/0`, IP Protocol `TCP`, Destination port
   `8090`. (Port 22 for SSH is already open.)
2. **OS side:** handled by the setup script below (Oracle's Ubuntu images ship a
   restrictive `iptables`).

## 4. Set up and launch (on the VM)
SSH in: `ssh ubuntu@<public-ip>` (user is `ubuntu` for Ubuntu images). Then:

```bash
# one-shot bootstrap: installs Docker, opens the OS firewall, clones the repo
curl -fsSL https://raw.githubusercontent.com/saimuu1/uptime-monitor/main/deploy/vm-setup.sh | bash
cd ~/uptime-monitor

# secrets: your page login + (optional) email
cp deploy/.env.example deploy/.env
nano deploy/.env          # set SMTP_* for email (you sign up for a login on the page)

# launch (builds images the first time, ~a few minutes)
sudo docker compose -f deploy/docker-compose.yml up -d --build
```

> The repo must be **public** for the `curl | bash` to work. Making it public is
> also good for your portfolio (no secrets are committed — `deploy/.env` and
> Terraform vars are gitignored). Otherwise, `git clone` it with a token first.

## 5. Use it
Open **`http://<public-ip>:8090`**, log in with WEB_USER/WEB_PASS, and add your
sites. It now runs 24/7 — put the URL on your résumé.

Free HTTPS + a real domain later: point a domain at the IP and run Caddy, or use a
free Cloudflare Tunnel.

## Managing it
```bash
sudo docker compose -f deploy/docker-compose.yml logs -f     # watch logs
sudo docker compose -f deploy/docker-compose.yml pull && \
  sudo docker compose -f deploy/docker-compose.yml up -d --build   # update after git pull
sudo docker compose -f deploy/docker-compose.yml down        # stop
```
