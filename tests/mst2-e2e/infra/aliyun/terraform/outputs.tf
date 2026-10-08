output "campaign" {
  description = "Public resource identities only; consumed by the external lifecycle controller."
  value = {
    run_id                     = var.run_id
    test_tier                  = var.test_tier
    region                     = var.region
    zone                       = var.zone
    session_started_utc        = var.session_started_utc
    measurement_deadline       = timeadd(var.session_started_utc, "220m")
    cleanup_deadline           = timeadd(var.session_started_utc, "235m")
    expires_at                 = var.expires_at
    instance_id                = alicloud_instance.benchmark.id
    private_ip                 = alicloud_instance.benchmark.private_ip
    public_ip                  = alicloud_instance.benchmark.public_ip
    vpc_id                     = alicloud_vpc.benchmark.id
    vswitch_id                 = alicloud_vswitch.benchmark.id
    security_group_id          = alicloud_security_group.benchmark.id
    ram_role_name              = alicloud_ram_role.benchmark.role_name
    ram_policy_name            = alicloud_ram_policy.evidence.policy_name
    ram_role_attachment_id     = alicloud_ecs_ram_role_attachment.benchmark.id
    evidence_bucket            = alicloud_oss_bucket.evidence.bucket
    source_prefix              = local.source_prefix
    evidence_prefix            = local.evidence_prefix
    evidence_endpoint          = "https://oss-${var.region}.aliyuncs.com"
    evidence_internal_endpoint = "https://oss-${var.region}-internal.aliyuncs.com"
    bootstrap_mount_root       = local.mount_root
    bootstrap_ready_path       = "/var/lib/scorpiofs-benchmark/bootstrap-ready.json"
    execution_mode             = "cloud-assistant"
    test_user                  = "benchmark"
    test_home                  = "${local.mount_root}/test-home"
    work_root                  = "${local.mount_root}/work"
    cargo_home                 = "${local.mount_root}/cargo"
    rustup_home                = "${local.mount_root}/rustup"
    temporary_root             = "${local.mount_root}/tmp"
    native_storage_backend     = "local"
  }
}
