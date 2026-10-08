locals {
  name = "scorpiofs-${var.run_id}"
  tags = {
    project    = "scorpiofs"
    purpose    = "v3-campaign"
    managed_by = "terraform"
    run_id     = var.run_id
    test_tier  = var.test_tier
    expires_at = var.expires_at
  }
  mount_root      = "/srv/scorpiofs-benchmark"
  source_prefix   = "sources/${var.run_id}/"
  evidence_prefix = "runs/${var.run_id}/"
  # All current tiers share the approved 8 vCPU / 32 GiB / 300 GiB minimum.
  # This map makes admission explicit without silently selecting a larger SKU.
  tier_shape = {
    smoke         = { logical_cpus = 8, memory_gib = 30, disk_free_bytes = 116412113924 }
    medium        = { logical_cpus = 8, memory_gib = 30, disk_free_bytes = 116412113924 }
    history-large = { logical_cpus = 8, memory_gib = 30, disk_free_bytes = 213559413282 }
  }
  # Windows checkouts may use CRLF; cloud-init must write a Linux executable.
  bootstrap = replace(templatefile("${path.module}/bootstrap.sh.tftpl", {
    run_id               = var.run_id
    test_tier            = var.test_tier
    minimum_cpus         = try(local.tier_shape[var.test_tier].logical_cpus, 8)
    minimum_memory_gib   = try(local.tier_shape[var.test_tier].memory_gib, 30)
    minimum_free_bytes   = try(local.tier_shape[var.test_tier].disk_free_bytes, 116412113924)
    session_started_utc  = var.session_started_utc
    session_deadline_utc = timeadd(var.session_started_utc, "235m")
    expires_at           = var.expires_at
    data_disk_device     = var.data_disk_device
    data_disk_size_gib   = var.data_disk_size_gib
  }), "\r\n", "\n")
}

# This module creates only new run-owned resources; it never imports a live stack.
resource "alicloud_vpc" "benchmark" {
  vpc_name   = local.name
  cidr_block = var.vpc_cidr
  tags       = local.tags
}

resource "alicloud_vswitch" "benchmark" {
  vswitch_name = local.name
  vpc_id       = alicloud_vpc.benchmark.id
  zone_id      = var.zone
  cidr_block   = var.vswitch_cidr
  tags         = local.tags
}

resource "alicloud_security_group" "benchmark" {
  security_group_name = local.name
  description         = "Disposable v3 benchmark; no public application ingress"
  vpc_id              = alicloud_vpc.benchmark.id
  tags                = local.tags
}

resource "alicloud_instance" "benchmark" {
  instance_name                 = local.name
  description                   = "Run-owned ephemeral Cloud Assistant host for the v3 campaign"
  instance_type                 = var.instance_type
  image_id                      = var.image_id
  vswitch_id                    = alicloud_vswitch.benchmark.id
  security_groups               = [alicloud_security_group.benchmark.id]
  instance_charge_type          = "PostPaid"
  internet_charge_type          = "PayByTraffic"
  internet_max_bandwidth_out    = var.internet_max_bandwidth_out
  system_disk_category          = "cloud_essd"
  system_disk_size              = 60
  system_disk_performance_level = "PL1"
  deletion_protection           = false
  http_endpoint                 = "enabled"
  http_tokens                   = "required"
  http_put_response_hop_limit   = 1

  # Supported by aliyun/alicloud 1.293.0. This releases ECS and its inline disk;
  # the external controller must still delete OSS, security group, and VPC.
  auto_release_time = var.expires_at
  tags              = local.tags
  volume_tags       = local.tags

  data_disks {
    name                 = "${local.name}-work"
    description          = "New empty campaign work disk; released with its instance"
    category             = "cloud_essd"
    size                 = var.data_disk_size_gib
    performance_level    = "PL1"
    device               = var.data_disk_device
    delete_with_instance = true
  }

  user_data = "#cloud-config\n${yamlencode({
    write_files = [{
      path        = "/usr/local/sbin/scorpiofs-benchmark-bootstrap"
      permissions = "0755"
      owner       = "root:root"
      content     = local.bootstrap
    }]
    runcmd = [["timeout", "--kill-after=30s", "900s", "/usr/local/sbin/scorpiofs-benchmark-bootstrap"]]
  })}"

  lifecycle {
    precondition {
      condition     = var.expires_at == timeadd(var.session_started_utc, "240m")
      error_message = "The hard release time must remain exactly four hours after the original first-create anchor."
    }
    precondition {
      condition     = startswith(var.zone, "${var.region}-")
      error_message = "zone must belong to the explicitly selected region."
    }
  }
}

# OSS transfers source bundles and evidence. Native Mega2 uses the Local store.
# Retrying a create must reconcile this exact name; never import an existing bucket.
resource "alicloud_oss_bucket" "evidence" {
  bucket        = "scorpiofs-bench-${var.run_id}"
  storage_class = "Standard"
  force_destroy = false
  tags          = local.tags

  server_side_encryption_rule {
    sse_algorithm = "AES256"
  }

  lifecycle_rule {
    id      = "run-results"
    enabled = true
    prefix  = local.evidence_prefix
    expiration {
      days = var.evidence_retention_days
    }
    abort_multipart_upload {
      days = 1
    }
  }

  lifecycle_rule {
    id      = "run-sources"
    enabled = true
    prefix  = local.source_prefix
    expiration {
      days = var.evidence_retention_days
    }
    abort_multipart_upload {
      days = 1
    }
  }
}

resource "alicloud_oss_bucket_acl" "evidence" {
  bucket = alicloud_oss_bucket.evidence.bucket
  acl    = "private"
}

resource "alicloud_oss_bucket_public_access_block" "evidence" {
  bucket              = alicloud_oss_bucket.evidence.bucket
  block_public_access = true
}

# ECS receives temporary role credentials, never the controller's account keys.
resource "alicloud_ram_role" "benchmark" {
  role_name = local.name
  assume_role_policy_document = jsonencode({
    Version = "1"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = ["ecs.aliyuncs.com"] }
    }]
  })
  max_session_duration = 3600
  force                = false
  tags                 = local.tags
}

resource "alicloud_ram_policy" "evidence" {
  policy_name = "${local.name}-transfer"
  policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["oss:GetObject"]
        Resource = ["acs:oss:*:*:${alicloud_oss_bucket.evidence.bucket}/${local.source_prefix}*"]
      },
      {
        Effect   = "Allow"
        Action   = ["oss:GetObject", "oss:PutObject"]
        Resource = ["acs:oss:*:*:${alicloud_oss_bucket.evidence.bucket}/${local.evidence_prefix}*"]
      }
    ]
  })
  force = false
  tags  = local.tags
}

resource "alicloud_ram_role_policy_attachment" "evidence" {
  policy_name = alicloud_ram_policy.evidence.policy_name
  policy_type = "Custom"
  role_name   = alicloud_ram_role.benchmark.role_name
}

resource "alicloud_ecs_ram_role_attachment" "benchmark" {
  instance_id   = alicloud_instance.benchmark.id
  ram_role_name = alicloud_ram_role.benchmark.role_name
  depends_on    = [alicloud_ram_role_policy_attachment.evidence]
}
