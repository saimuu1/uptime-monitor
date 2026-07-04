variable "do_token" {
  description = "DigitalOcean API token (create at cloud.digitalocean.com/account/api)."
  type        = string
  sensitive   = true
}

variable "ssh_key_ids" {
  description = "IDs/fingerprints of DO SSH keys to add to every droplet (so you can log in)."
  type        = list(string)
  default     = []
}

variable "core_region" {
  description = "Region for the central box (db, queue, scheduler, evaluator, web)."
  type        = string
  default     = "nyc1"
}

variable "checker_regions" {
  description = "One checker droplet is created per region here — the real 'multi-region'."
  type        = list(string)
  default     = ["nyc1", "lon1", "sgp1"]
}

variable "droplet_size" {
  description = "DO droplet size slug (s-1vcpu-1gb is the cheapest useful box)."
  type        = string
  default     = "s-1vcpu-1gb"
}

variable "image_registry" {
  description = "Registry/namespace holding the pushed images, e.g. ghcr.io/<user>."
  type        = string
  default     = "ghcr.io/saimuu1"
}

variable "image_tag" {
  description = "Image tag to deploy."
  type        = string
  default     = "latest"
}

variable "config_yaml" {
  description = "Contents of config.yaml (the monitor list) placed on the core box."
  type        = string
  default     = <<-EOT
    monitors:
      - name: "Example"
        url: "https://example.com"
        interval_seconds: 30
  EOT
}

variable "alert_webhook_url" {
  description = "Discord/Slack webhook for alerts. Empty = log only."
  type        = string
  default     = ""
  sensitive   = true
}

variable "allowed_web_cidrs" {
  description = "Who may reach the status page (port 8090)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
