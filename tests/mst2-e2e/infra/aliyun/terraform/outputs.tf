output "campaign" {
  description = "Public resource identities only; consumed by the external lifecycle controller."
  value = {
    run_id                 = var.run_id
    region                 = var.region
    zone                   = var.zone
    session_started_utc    = var.session_started_utc
    measurement_deadline   = timeadd(var.session_started_utc, "220m")
    cleanup_deadline       = timeadd(var.session_started_utc, "235m")
    expires_at             = var.expires_at
    instance_id            = alicloud_instance.benchmark.id
    private_ip             = alicloud_instance.benchmark.private_ip
    public_ip              = alicloud_instance.benchmark.public_ip
    vpc_id                 = alicloud_vpc.benchmark.id
    vswitch_id             = alicloud_vswitch.benchmark.id
    security_group_id      = alicloud_security_group.benchmark.id
    key_pair_name          = var.ssh_public_key == "" ? null : alicloud_ecs_key_pair.operator[0].key_pair_name
    evidence_bucket        = alicloud_oss_bucket.evidence.bucket
    evidence_prefix        = local.evidence_prefix
    evidence_endpoint      = "https://oss-${var.region}.aliyuncs.com"
    bootstrap_mount_root   = local.mount_root
    bootstrap_ready_path   = "/var/lib/scorpiofs-benchmark/bootstrap-ready.json"
    runner_user            = "benchmark"
    runner_label           = local.name
    runner_install_root    = "${local.mount_root}/runner"
    runner_work_root       = "${local.mount_root}/work"
    cargo_home             = "${local.mount_root}/cargo"
    rustup_home            = "${local.mount_root}/rustup"
    temporary_root         = "${local.mount_root}/tmp"
    native_storage_backend = "local"
  }
}
