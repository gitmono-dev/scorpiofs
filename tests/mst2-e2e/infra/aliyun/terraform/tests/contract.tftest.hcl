mock_provider "alicloud" {}

variables {
  region              = "cn-hangzhou"
  zone                = "cn-hangzhou-h"
  image_id            = "ubuntu_22_04_x64_reviewed_image"
  instance_type       = "ecs.u1-c1m4.2xlarge"
  run_id              = "offline-contract-20261008"
  session_started_utc = "2026-10-08T00:00:00Z"
  expires_at          = "2026-10-08T04:00:00Z"
  vpc_cidr            = "10.71.0.0/16"
  vswitch_cidr        = "10.71.1.0/24"
}

run "fixed_window_and_run_owned_disk" {
  command = plan
  assert {
    condition     = alicloud_instance.benchmark.auto_release_time == "2026-10-08T04:00:00Z" && alicloud_instance.benchmark.data_disks[0].delete_with_instance
    error_message = "The ECS hard release and inline work disk must retain the original immutable deadline."
  }
  assert {
    condition     = output.campaign.cleanup_deadline == "2026-10-08T03:55:00Z" && output.campaign.measurement_deadline == "2026-10-08T03:40:00Z"
    error_message = "Work and cleanup must share the original four-hour anchor."
  }
  assert {
    condition     = length(alicloud_security_group_rule.ssh) == 0 && length(alicloud_ecs_key_pair.operator) == 0 && !alicloud_oss_bucket.evidence.force_destroy
    error_message = "Default deployment must not open SSH, generate a key, or force-delete result objects."
  }
  assert {
    condition     = alicloud_oss_bucket_acl.evidence.acl == "private" && alicloud_oss_bucket_public_access_block.evidence.block_public_access && alicloud_oss_bucket.evidence.server_side_encryption_rule[0].sse_algorithm == "AES256"
    error_message = "Result archives require private access, public-access blocking, and server-side encryption."
  }
  assert {
    condition     = can(regex("readonly DEADLINE_UTC='2026-10-08T03:55:00Z'", local.bootstrap)) && can(regex("\\$\\{OWNER\\[0\\]\\}", local.bootstrap))
    error_message = "The rendered bootstrap must preserve the session window and shell array references."
  }
}

run "reject_extended_lease" {
  command = plan
  variables {
    expires_at = "2026-10-08T04:01:00Z"
  }
  expect_failures = [alicloud_instance.benchmark]
}

run "reject_other_region_zone" {
  command = plan
  variables {
    zone = "cn-beijing-a"
  }
  expect_failures = [alicloud_instance.benchmark]
}

run "reject_broad_ssh_ingress" {
  command = plan
  variables {
    ssh_operator_cidrs = ["0.0.0.0/0"]
  }
  expect_failures = [var.ssh_operator_cidrs]
}

run "reject_ingress_without_public_key" {
  command = plan
  variables {
    ssh_operator_cidrs = ["203.0.113.2/32"]
  }
  expect_failures = [alicloud_instance.benchmark]
}

run "reject_system_disk" {
  command = plan
  variables {
    data_disk_device = "/dev/vda"
  }
  expect_failures = [var.data_disk_device]
}

run "reject_undersized_work_disk" {
  command = plan
  variables {
    data_disk_size_gib = 100
  }
  expect_failures = [var.data_disk_size_gib]
}

run "reject_private_key_input" {
  command = plan
  variables {
    ssh_public_key = "-----BEGIN OPENSSH PRIVATE KEY-----"
  }
  expect_failures = [var.ssh_public_key]
}
