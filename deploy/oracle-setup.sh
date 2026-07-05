#!/usr/bin/env bash
# Bootstrap an Oracle Cloud (or any Ubuntu) VM to run the uptime monitor.
# Run it on the VM as the default user:
#   curl -fsSL https://raw.githubusercontent.com/saimuu1/uptime-monitor/main/deploy/oracle-setup.sh | bash
# (requires the repo to be public; otherwise git clone it yourself first)
set -euo pipefail

REPO="${REPO:-https://github.com/saimuu1/uptime-monitor}"
WEB_PORT="${WEB_PORT:-8090}"

echo "==> Installing Docker, compose plugin, git..."
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
	docker.io docker-compose-v2 git iptables-persistent
sudo systemctl enable --now docker

echo "==> Opening port ${WEB_PORT} in the OS firewall (Oracle's Ubuntu blocks it by default)..."
# Insert before Oracle's default REJECT rule.
sudo iptables -I INPUT -p tcp --dport "${WEB_PORT}" -j ACCEPT
sudo netfilter-persistent save || true

echo "==> Cloning the repo..."
cd "$HOME"
[ -d uptime-monitor ] || git clone "$REPO"
cd uptime-monitor

cat <<EOF

==========================================================
 VM is ready. Two steps left:

 1) Configure secrets:
      cp deploy/.env.example deploy/.env
      nano deploy/.env
    Set WEB_USER + WEB_PASS (your page login) and, for email
    alerts, SMTP_USER / SMTP_FROM / SMTP_PASS (Gmail app password).

 2) Launch (builds the images, ~a few minutes the first time):
      sudo docker compose -f deploy/docker-compose.yml up -d --build

 Then open:  http://<THIS-VM-PUBLIC-IP>:${WEB_PORT}
 (also add an Ingress rule for ${WEB_PORT} in the Oracle VCN Security List)
==========================================================
EOF
