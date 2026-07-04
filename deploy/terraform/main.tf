# The central box: runs the database, the NATS queue, and the scheduler /
# evaluator / web services (everything except the regional checkers).
resource "digitalocean_droplet" "core" {
  name     = "uptime-core"
  region   = var.core_region
  size     = var.droplet_size
  image    = "ubuntu-24-04-x64"
  ssh_keys = var.ssh_key_ids
  tags     = ["uptime", "uptime-core"]

  user_data = templatefile("${path.module}/cloud-init-core.yaml.tftpl", {
    compose_b64 = base64encode(templatefile("${path.module}/compose-core.yml.tftpl", {
      image_registry    = var.image_registry
      image_tag         = var.image_tag
      alert_webhook_url = var.alert_webhook_url
    }))
    config_b64 = base64encode(var.config_yaml)
  })
}

# One checker per region. Each runs the checker container, tagged with its
# region, pointed at the core box's NATS. This is what makes consensus real.
resource "digitalocean_droplet" "checker" {
  for_each = toset(var.checker_regions)

  name     = "uptime-checker-${each.key}"
  region   = each.key
  size     = var.droplet_size
  image    = "ubuntu-24-04-x64"
  ssh_keys = var.ssh_key_ids
  tags     = ["uptime", "uptime-checker"]

  user_data = templatefile("${path.module}/cloud-init-checker.yaml.tftpl", {
    region         = each.key
    core_ip        = digitalocean_droplet.core.ipv4_address
    image_registry = var.image_registry
    image_tag      = var.image_tag
  })
}

# Firewall on the core box: SSH from anywhere, the status page from the allowed
# CIDRs, and the NATS port only from the checker droplets.
resource "digitalocean_firewall" "core" {
  name        = "uptime-core-fw"
  droplet_ids = [digitalocean_droplet.core.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "8090"
    source_addresses = var.allowed_web_cidrs
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "4222"
    source_addresses = [for d in digitalocean_droplet.checker : "${d.ipv4_address}/32"]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
