locals {
  name = "scorpiofs-${var.run_id}"
  tags = {
    project    = "scorpiofs"
    purpose    = "v3-campaign"
    managed_by = "terraform"
    run_id     = var.run_id
    expires_at = var.expires_at
  }
  mount_root      = "/srv/scorpiofs-benchmark"
  evidence_prefix = "runs/${var.run_id}/"
  bootstrap = templatefile("${path.module}/bootstrap.sh.tftpl", {
    run_id               = var.run_id
    session_started_utc  = var.session_started_utc
    session_deadline_utc = timeadd(var.session_started_utc, "235m")
    expires_at           = var.expires_at
    data_disk_device     = var.data_disk_device
    data_disk_size_gib   = var.data_disk_size_gib
  })
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

resource "alicloud_security_group_rule" "ssh" {
  for_each          = var.ssh_operator_cidrs
  security_group_id = alicloud_security_group.benchmark.id
  type              = "ingress"
  ip_protocol       = "tcp"
  nic_type          = "intranet"
  policy            = "accept"
  port_range        = "22/22"
  cidr_ip           = each.value
}

resource "alicloud_ecs_key_pair" "operator" {
  count         = var.ssh_public_key == "" ? 0 : 1
  key_pair_name = local.name
  public_key    = var.ssh_public_key
  tags          = local.tags
}

resource "alicloud_instance" "benchmark" {
  instance_name                 = local.name
  description                   = "Run-owned ephemeral Linux Actions host for the v3 campaign"
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
  key_name                      = var.ssh_public_key == "" ? null : alicloud_ecs_key_pair.operator[0].key_pair_name
  deletion_protection           = false

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
    precondition {
      condition     = (length(var.ssh_operator_cidrs) == 0) == (var.ssh_public_key == "")
      error_message = "SSH requires both an existing public key and an explicit /32 operator whitelist."
    }
  }
}

# OSS is only the evidence archive. Native Mega2 uses the isolated Local store.
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
}

resource "alicloud_oss_bucket_acl" "evidence" {
  bucket = alicloud_oss_bucket.evidence.bucket
  acl    = "private"
}

resource "alicloud_oss_bucket_public_access_block" "evidence" {
  bucket              = alicloud_oss_bucket.evidence.bucket
  block_public_access = true
}
