mock_provider "alicloud" {}

variables {
  region              = "cn-hangzhou"
  zone                = "cn-hangzhou-h"
  image_id            = "ubuntu_24_04_x64_reviewed_image"
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
    condition     = alicloud_instance.benchmark.system_disk_category == "cloud_essd" && alicloud_instance.benchmark.system_disk_size == 60 && alicloud_instance.benchmark.data_disks[0].category == "cloud_essd" && alicloud_instance.benchmark.data_disks[0].size == 300
    error_message = "The approved default is one 60 GiB system ESSD and one 300 GiB local work ESSD."
  }
  assert {
    condition     = !alicloud_oss_bucket.evidence.force_destroy && alicloud_oss_bucket_acl.evidence.acl == "private" && alicloud_oss_bucket_public_access_block.evidence.block_public_access && alicloud_oss_bucket.evidence.server_side_encryption_rule[0].sse_algorithm == "AES256"
    error_message = "Archives require private encrypted storage and explicit object cleanup."
  }
  assert {
    condition     = output.campaign.test_tier == "history-large" && output.campaign.native_storage_backend == "local" && output.campaign.execution_mode == "cloud-assistant" && output.campaign.test_user == "benchmark" && output.campaign.test_home == "/srv/scorpiofs-benchmark/test-home"
    error_message = "Default deployment must select the direct history-large local-store campaign."
  }
  assert {
    condition     = can(regex("readonly DEADLINE_UTC='2026-10-08T03:55:00Z'", local.bootstrap)) && can(regex("\\$\\{OWNER\\[0\\]\\}", local.bootstrap)) && can(regex("readonly TEST_TIER='history-large'", local.bootstrap)) && !strcontains(local.bootstrap, "\r")
    error_message = "The bootstrap must preserve the session window, selected tier, and shell array references."
  }
}

run "direct_access_and_temporary_identity" {
  command = plan
  assert {
    condition     = !strcontains(file("${path.module}/main.tf"), "resource \"alicloud_security_group_rule\"") && !strcontains(file("${path.module}/main.tf"), "resource \"alicloud_ecs_key_pair\"")
    error_message = "The module must not create inbound access rules or an SSH key."
  }
  assert {
    condition     = alicloud_instance.benchmark.http_endpoint == "enabled" && alicloud_instance.benchmark.http_tokens == "required" && alicloud_instance.benchmark.http_put_response_hop_limit == 1
    error_message = "Temporary ECS credentials must use IMDSv2 with a one-hop response limit."
  }
  assert {
    condition = jsondecode(alicloud_ram_role.benchmark.assume_role_policy_document) == {
      Version = "1"
      Statement = [{
        Effect    = "Allow"
        Action    = "sts:AssumeRole"
        Principal = { Service = ["ecs.aliyuncs.com"] }
      }]
    }
    error_message = "Only the official ECS service may assume the ephemeral campaign role."
  }
  assert {
    condition     = alicloud_ecs_ram_role_attachment.benchmark.ram_role_name == alicloud_ram_role.benchmark.role_name && alicloud_ram_role_policy_attachment.evidence.role_name == alicloud_ram_role.benchmark.role_name && alicloud_ram_role_policy_attachment.evidence.policy_type == "Custom" && !alicloud_ram_role.benchmark.force && !alicloud_ram_policy.evidence.force
    error_message = "The instance role must attach only its custom transfer policy and support explicit cleanup."
  }
  assert {
    condition     = output.campaign.source_prefix == "sources/offline-contract-20261008/" && output.campaign.evidence_prefix == "runs/offline-contract-20261008/" && output.campaign.evidence_internal_endpoint == "https://oss-cn-hangzhou-internal.aliyuncs.com"
    error_message = "Source and evidence transfers must use their run-owned paths and region endpoint."
  }
}

run "least_privilege_source_and_evidence_policy" {
  command = plan
  assert {
    condition = jsondecode(alicloud_ram_policy.evidence.policy_document) == {
      Version = "1"
      Statement = [
        {
          Effect   = "Allow"
          Action   = ["oss:GetObject"]
          Resource = ["acs:oss:*:*:scorpiofs-bench-offline-contract-20261008/sources/offline-contract-20261008/*"]
        },
        {
          Effect   = "Allow"
          Action   = ["oss:GetObject", "oss:PutObject"]
          Resource = ["acs:oss:*:*:scorpiofs-bench-offline-contract-20261008/runs/offline-contract-20261008/*"]
        }
      ]
    }
    error_message = "The ECS role may only read this run's sources and read/write this run's evidence objects."
  }
  assert {
    condition     = length(alicloud_oss_bucket.evidence.lifecycle_rule) == 2 && toset([for rule in alicloud_oss_bucket.evidence.lifecycle_rule : rule.prefix]) == toset([local.source_prefix, local.evidence_prefix]) && alltrue([for rule in alicloud_oss_bucket.evidence.lifecycle_rule : rule.enabled && one(rule.expiration).days == 7 && one(rule.abort_multipart_upload).days == 1])
    error_message = "Both run-owned prefixes require fallback expiry and abandoned multipart cleanup."
  }
}

run "smoke_tier_admission" {
  command = plan
  variables {
    test_tier = "smoke"
  }
  assert {
    condition     = output.campaign.test_tier == "smoke" && alicloud_instance.benchmark.instance_type == "ecs.u1-c1m4.2xlarge" && can(regex("readonly TEST_TIER='smoke'", local.bootstrap))
    error_message = "Smoke selects a single explicit tier without increasing the approved instance size."
  }
}

run "medium_tier_admission" {
  command = plan
  variables {
    test_tier = "medium"
  }
  assert {
    condition     = output.campaign.test_tier == "medium" && alicloud_instance.benchmark.instance_type == "ecs.u1-c1m4.2xlarge" && can(regex("readonly TEST_TIER='medium'", local.bootstrap))
    error_message = "Medium selects a single explicit tier without increasing the approved instance size."
  }
}

run "reject_unknown_tier" {
  command = plan
  variables {
    test_tier = "large"
  }
  expect_failures = [var.test_tier]
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
