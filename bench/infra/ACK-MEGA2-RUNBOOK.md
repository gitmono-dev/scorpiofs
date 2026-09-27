# ACK 部署 mega2：可行方案（2026-09-27 验证通过）

## 结论

在 ACK 上跑 mega2 **可行，且已验证**（clone → commit → push → 服务端回读 → 重新克隆全部通过）。

**唯一的关键点：镜像必须从 ACR 拉，不能从 VPC 内自建 registry 拉。**

- ❌ VPC 内跑 `registry:2` → ACK 节点 `dial tcp i/o timeout`（即使 ping 通、安全组全放行）
- ✅ ACR 个人版内网端点 `crpi-<id>-vpc.cn-hangzhou.personal.cr.aliyuncs.com`
  → 节点天然可达（ACK 自己的系统组件也走同类 `registry-*-vpc.ack.aliyuncs.com` 端点）

## 完整流程

```bash
# 1) 建集群（控制面免费）
#    注意：顶层 num_of_nodes / worker_instance_types / nodepools 字段
#    在 CreateCluster 里都可能被忽略 —— 见下方"坑"
aliyun cs POST /clusters --body @ack-create.json
#    关键参数：snat_entry=true（worker 需要出网）、endpoint_public_access=true

# 2) 等集群 running → 取 kubeconfig
aliyun cs GET /k8s/<cluster-id>/user_config     # cluster-id 就是 context 名后缀
export KUBECONFIG=~/gitmono-ack.conf

# 3) 建节点池（用独立 API，不要指望 CreateCluster 里的 nodepools 生效）
aliyun cs POST /clusters/<cid>/nodepools --body @ack-nodepool.json

# 4) 等节点 Ready，然后部署
ACR_PASS=<registry密码> bash bench/infra/ack-deploy.sh
```

## 镜像准备（必须先做）

```bash
# 自建的两个 + 4 个公开基础镜像，全部推到 ACR
ACR_PASS=<pw> bash bench/infra/push-to-acr-personal.sh    # mega2, scorpiofs
ACR_PASS=<pw> bash bench/infra/push-bases-to-acr.sh       # postgres, redis, rustfs, rustfs-rc
```

## 踩过的坑（每个都真实发生过）

| # | 坑 | 症状 / 修法 |
|---|---|---|
| 1 | **Secret 必须在 Namespace 之后创建** | 顺序颠倒 → `namespaces "gitmono" not found` → 所有 pod `ImagePullBackOff`（没有拉取凭据） |
| 2 | `Secret` 不能把 JSON 对象内嵌进 `stringData` | `cannot unmarshal object into Go struct field Secret.stringData of type string`。改用 `kubectl create secret docker-registry ... --dry-run=client -o yaml \| kubectl apply -f -` |
| 3 | **ACK 会自动创建 `default-nodepool`** | 我建的池 + ACK 自建的池同时存在，共 3 个池 → 起了 4 台 ECS（成本翻倍）。必须逐个缩容 |
| 4 | `CreateCluster` 顶层的 `nodepools` 数组**不生效** | 创建后查节点池为 0 个；必须用 `POST /clusters/<cid>/nodepools` |
| 5 | 节点池缩容期间**不能改配置** | `InvalidNodePoolStatus.Forbidden: cannot operate nodepool when nodepool state is removing_nodes`，要等回 `active` |
| 6 | 节点池 `desired_size=1` 可能实际起 2 台 | 显式设 `min_size=max_size=desired_size` 约束 |
| 7 | Pod 卡 `Pending` 而非 `ImagePullBackOff` | 说明**节点被缩容收走了**——调试和止损会互相冲突，别同时做 |
| 8 | 给 ACK 节点安全组加自定义 egress 规则**会覆盖默认全放行** | 阿里云语义：一旦有自定义 egress，其余全部拒绝 → 把节点的 DNS/公网出口断掉 |

## 成本

| 项 | 约 |
|---|---|
| ACK 控制面（ack.standard） | **免费** |
| worker 4c8g (ecs.e-c1m2.xlarge) 按量 | ~¥0.7/小时 |
| NAT 网关（snat_entry=true，ACK 建） | ~¥0.5/小时 |
| EIP（NAT 用） | 少量流量费 |

**验证完立即删除集群**（`aliyun cs DELETE /clusters/<cid>`），几分钟内 SLB/EIP/NAT 随之释放。

## 待办：ScorpioFS

本次只跑了 mega2（git 服务端）。ScorpioFS 容器还需要：
- `securityContext.privileged: true` + `hostPath /dev/fuse`
- `SCORPIO_MOUNT_OWNER`（容器内以 root 跑、无 SUDO_USER，否则 copy-up 产物属主错误）
- 命名空间 PSA 标签：`pod-security.kubernetes.io/enforce=privileged`
