#!/usr/bin/env bash
# Bootstrap any Ubuntu VM (Azure, Oracle, GCP, AWS, ...) to run the uptime monitor.
# Run it on the VM as the default user:
#   curl -fsSL https://raw.githubusercontent.com/saimuu1/uptime-monitor/main/deploy/vm-setup.sh | bash
# (requires the repo to be public; otherwise git clone it yourself first)
set -euo pipefail

REPO="${REPO:-https://github.com/saimuu1/uptime-monitor}"
WEB_PORT="${WEB_PORT:-8090}"

echo "==> Installing Docker, compose plugin, git..."
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
	docker.io docker-compose-v2 git iptables-persistent
sudo systemctl enable --now docker

# Small free-tier VMs (1 GB) can't compile the Go services without swap.
if [ "$(swapon --show | wc -l)" -eq 0 ]; then
	echo "==> No swap found; adding 2G (needed to build on 1 GB machines)..."
	sudo fallocate -l 2G /swapfile
	sudo chmod 600 /swapfile
	sudo mkswap /swapfile
	sudo swapon /swapfile
	echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

echo "==> Opening port ${WEB_PORT} in the OS firewall (some images block it by default)..."
sudo iptables -I INPUT -p tcp --dport "${WEB_PORT}" -j ACCEPT
sudo netfilter-persistent save || true

echo "==> Cloning the repo..."
cd "$HOME"
[ -d uptime-monitor ] || git clone "$REPO"
cd uptime-monitor

cat <<EOF

==========================================================
 VM is ready. Two steps left:

 1) Configure email alerts (optional):
      cp deploy/.env.example deploy/.env
      nano deploy/.env
    Set SMTP_USER / SMTP_FROM / SMTP_PASS (Gmail app password).
    You create your login by signing up on the page itself.

 2) Launch. On a small VM the first build takes 10-20 min, so run it
    detached — that way a dropped SSH session won't kill it:
      nohup sudo docker compose -f deploy/docker-compose.yml up -d --build > ~/build.log 2>&1 &
      tail -f ~/build.log        # watch progress (Ctrl+C stops watching only)

 Then open:  http://<THIS-VM-PUBLIC-IP>:${WEB_PORT}
 (make sure port ${WEB_PORT} is open in your cloud's firewall:
  GCP = VPC firewall rule, Azure = NSG rule, Oracle = VCN security list)
==========================================================
EOF
