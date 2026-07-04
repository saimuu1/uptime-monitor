output "core_ip" {
  description = "Public IP of the central box."
  value       = digitalocean_droplet.core.ipv4_address
}

output "status_url" {
  description = "Open this once the box has booted and pulled images."
  value       = "http://${digitalocean_droplet.core.ipv4_address}:8090"
}

output "checker_ips" {
  description = "Public IP of each regional checker."
  value       = { for region, d in digitalocean_droplet.checker : region => d.ipv4_address }
}
