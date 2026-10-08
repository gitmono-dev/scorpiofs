terraform {
  required_version = ">= 1.9.8, < 2.0.0"

  required_providers {
    alicloud = {
      source  = "aliyun/alicloud"
      version = "= 1.293.0"
    }
  }
}

# Authentication uses the provider's standard environment or workload identity.
# No access key, runner token, or private SSH key is accepted by this module.
provider "alicloud" {
  region = var.region
}
