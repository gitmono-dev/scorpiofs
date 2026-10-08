variable "region" {
  description = "Explicit Alibaba Cloud region, for example cn-hangzhou."
  type        = string
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z0-9-]+$", var.region))
    error_message = "region must be an explicit Alibaba Cloud region ID."
  }
}

variable "zone" {
  description = "Availability zone for the new vSwitch and benchmark ECS."
  type        = string
}

variable "image_id" {
  description = "Explicit Ubuntu 22.04 or 24.04 amd64 cloud image ID."
  type        = string
  validation {
    condition     = length(trimspace(var.image_id)) > 3 && !strcontains(var.image_id, "\n")
    error_message = "image_id must name an explicitly reviewed Ubuntu amd64 image."
  }
}

variable "instance_type" {
  description = "Explicit dedicated x86 instance type with at least 8 vCPU and 32 GiB RAM."
  type        = string
  validation {
    condition     = can(regex("^ecs\\.[a-z0-9.-]+$", var.instance_type))
    error_message = "instance_type must be an explicit ECS type; runtime shape is checked before admission."
  }
}

variable "run_id" {
  description = "Globally unique lowercase campaign identity, fixed before the first create."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{5,31}$", var.run_id)) && !endswith(var.run_id, "-")
    error_message = "run_id must contain 6 to 32 lowercase letters, digits, or hyphens and start with a letter."
  }
}

variable "session_started_utc" {
  description = "Original first-create UTC anchor; never reset on retry."
  type        = string
  validation {
    condition     = can(regex("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", var.session_started_utc)) && can(timeadd(var.session_started_utc, "0s"))
    error_message = "session_started_utc must be an explicit UTC timestamp with seconds."
  }
}

variable "expires_at" {
  description = "Immutable hard ECS release time, exactly 240 minutes after the first-create anchor."
  type        = string
  validation {
    condition     = can(regex("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", var.expires_at)) && can(timeadd(var.expires_at, "0s"))
    error_message = "expires_at must be an explicit UTC timestamp with seconds."
  }
}

variable "vpc_cidr" {
  description = "Explicit IPv4 CIDR for a new run-owned VPC."
  type        = string
  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR."
  }
}

variable "vswitch_cidr" {
  description = "Explicit IPv4 subnet inside vpc_cidr, checked by the controller before apply."
  type        = string
  validation {
    condition     = can(cidrnetmask(var.vswitch_cidr))
    error_message = "vswitch_cidr must be a valid IPv4 CIDR."
  }
}

variable "data_disk_size_gib" {
  description = "Run-owned ESSD PL1 work disk; source, Docker, Cargo, and runner work share this disk."
  type        = number
  default     = 300
  validation {
    condition     = var.data_disk_size_gib >= 300 && var.data_disk_size_gib <= 500 && floor(var.data_disk_size_gib) == var.data_disk_size_gib
    error_message = "The bounded campaign requires an integer work disk size between 300 and 500 GiB."
  }
}

variable "data_disk_device" {
  description = "Explicit newly created virtio data disk; system disk and NVMe guessing are forbidden."
  type        = string
  default     = "/dev/vdb"
  validation {
    condition     = can(regex("^/dev/vd[b-z]$", var.data_disk_device))
    error_message = "data_disk_device must name the newly created virtio disk, such as /dev/vdb."
  }
}

variable "internet_max_bandwidth_out" {
  description = "PayByTraffic public egress cap for dependency downloads and Actions; no EIP/NAT is created."
  type        = number
  default     = 20
  validation {
    condition     = var.internet_max_bandwidth_out >= 1 && var.internet_max_bandwidth_out <= 100 && floor(var.internet_max_bandwidth_out) == var.internet_max_bandwidth_out
    error_message = "Public egress must be an integer from 1 to 100 Mbps."
  }
}

variable "ssh_operator_cidrs" {
  description = "Optional single IPv4 operator addresses; SSH is closed by default."
  type        = set(string)
  default     = []
  validation {
    condition     = alltrue([for value in var.ssh_operator_cidrs : can(cidrnetmask(value)) && endswith(value, "/32")])
    error_message = "Every optional SSH source must be an explicit IPv4 /32."
  }
}

variable "ssh_public_key" {
  description = "Optional existing public key to import; no private key is generated or written."
  type        = string
  default     = ""
  validation {
    condition     = var.ssh_public_key == "" || (can(regex("^(ssh-ed25519|ssh-rsa) [A-Za-z0-9+/]+={0,3}( [^\\r\\n]+)?$", var.ssh_public_key)) && !strcontains(var.ssh_public_key, "PRIVATE"))
    error_message = "Provide only a single-line existing OpenSSH public key, or leave SSH disabled."
  }
}

variable "evidence_retention_days" {
  description = "Backup expiry for run-owned result objects; normal cleanup explicitly empties the bucket."
  type        = number
  default     = 7
  validation {
    condition     = var.evidence_retention_days >= 1 && var.evidence_retention_days <= 30 && floor(var.evidence_retention_days) == var.evidence_retention_days
    error_message = "Result retention must be an integer from 1 to 30 days."
  }
}
