# Deploy free on Azure (student, no credit card)

**Azure for Students** gives you **$100 of credit with no credit card** — enough to
run this 24/7 for months. Steps:

## 1. Get the free student account
Go to [azure.microsoft.com/free/students](https://azure.microsoft.com/free/students/)
→ **Start free**. Verify with your school email. You get $100 credit, no card.

## 2. Create the VM
Azure Portal → **Create a resource → Virtual machine**:
- **Image:** Ubuntu Server 24.04 LTS.
- **Size:** `B1ms` (2 GB RAM) is a good balance — comfortably fits the stack and the
  $100 credit lasts ~6 months. (`B1s`/1 GB works too but is tight.)
- **Authentication:** SSH public key (let Azure generate one and download it, or
  paste your own).
- **Inbound ports:** allow **SSH (22)** for now.
- Create it, then copy the **Public IP address**.

## 3. Open the web port
VM → **Networking** → **Add inbound port rule**: Destination port `8090`, TCP,
Source `Any`, Allow. (SSH/22 is already open.)

## 4. Set up and launch (on the VM)
SSH in (`ssh azureuser@<public-ip>` — user is `azureuser` by default). Then:

```bash
# installs Docker, opens the OS firewall, clones the repo
curl -fsSL https://raw.githubusercontent.com/saimuu1/uptime-monitor/main/deploy/vm-setup.sh | bash
cd ~/uptime-monitor

# secrets: your email settings (optional but recommended)
cp deploy/.env.example deploy/.env
nano deploy/.env          # set SMTP_USER / SMTP_FROM / SMTP_PASS for email

# launch (builds images the first time, ~a few minutes)
sudo docker compose -f deploy/docker-compose.yml up -d --build
```

> The repo must be **public** for the `curl | bash` to work (no secrets are
> committed — `deploy/.env` and Terraform vars are gitignored). Otherwise
> `git clone` it with a token first.

## 5. Use it
Open **`http://<public-ip>:8090`**, sign up, and add your sites. It now runs 24/7.

For a real **HTTPS** address (so logins are encrypted) and a nicer URL, put a free
**Cloudflare Tunnel** or **Caddy + a domain** in front — ask and we'll set it up.

## Managing it
```bash
sudo docker compose -f deploy/docker-compose.yml logs -f      # watch logs
cd ~/uptime-monitor && git pull && \
  sudo docker compose -f deploy/docker-compose.yml up -d --build   # update
sudo docker compose -f deploy/docker-compose.yml down         # stop
```
