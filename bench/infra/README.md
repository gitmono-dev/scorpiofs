# bench/infra — 阿里云环境搭建全流程

> 目标：1 个控制面节点（mega2 + gitea + 观测）+ 2 个 runner ECS（mono / git），同 AZ。
> k8s 形态见文末（可选）；默认 docker compose（实验期最省事，变量最少）。

## 0. 开机器（aliyun CLI，按量付费，用完即释放）

```bash
# 已配置 aliyun CLI（aliyun configure）后：
aliyun ecs RunInstances \
  --RegionId cn-hangzhou-i --InstanceType ecs.c7.xlarge \   # 8c16g，runner
  --Amount 2 --InternetMaxBandwidthOut 5 \
  --SecurityGroupId <sg-id> --VSwitchId <vswitch-id> \
  --ImageId ubuntu_22_04_x64_20G_alibase_20240620.vhd \
  --InstanceChargeType PostPaid --KeyName <keypair>
# 控制面节点可用 ecs.c7.xlarge + 300GB ESSD 云盘
```

规格建议：
| 节点 | 规格 | 盘 |
|---|---|---|
| 控制面（mega2+gitea+观测） | 8c16g | 300GB ESSD PL0 |
| runner-mono | 8c16g | 200GB ESSD |
| runner-git | 8c16g | 200GB ESSD |

安全组：放行 19000/19700（mega2）、30080/30022（gitea）、9090/9100/3000（观测）——**仅限 VPC 内**，勿开公网。

## 1. 控制面

```bash
# mega2：本地已验证的镜像直传（或云上构建）
docker save mega2-mst2:local | gzip > mega2-mst2.tar.gz
scp mega2-mst2.tar.gz node-a:/tmp/   # node-a 上 docker load
# node-a 上（用 scorpiofs 仓库的 compose + mst2 override，端口 19000/19700）
docker compose -f mega2-compose.yml -p bench up -d --wait

# gitea + prometheus + grafana + node-exporter
cp -r bench/infra . && cd infra
docker compose -f docker-compose.infra.yml -p bench-infra up -d --wait
# gitea 初始化: 访问 :30080 注册 bench 用户 → 生成 token → 建 org `bench`
```

## 2. runner 初始化

```bash
# runner-mono
MEGA_URL=http://<node-a-ip>:19000 bash init-runner-mono.sh
sudo systemctl start scorpio
curl -s http://127.0.0.1:37251/antares/health | jq .capabilities

# runner-git
GITEA=http://<node-a-ip>:30080 GITEA_TOKEN=xxx bash init-runner-git.sh
```

## 3. workload 与实验

```bash
# 合成仓库（runner-mono 上生成一次，两边同源）
bash workload/gen-repo.sh --out /tmp/synth-100k --files 100000
bash workload/seed-mono.sh --src /tmp/synth-100k --name synth100k
scp /tmp/synth-100k runner-git:/tmp/   # git 侧同一棵树
bash workload/seed-git.sh --tree /tmp/synth-100k --repo synth100k

# 实验一/二/三（见 ../TEST-PLAN.md 用例矩阵）
ROUNDS=5 bash cases/exp1.sh all
ROUNDS=5 bash cases/exp2.sh all
ROUNDS=3 bash cases/exp3.sh init multi switch
python3 bin/report.py
```

## 4. k8s 形态（可选）

`mega2-k8s.yaml` 提供 mega2/gitea/prometheus 的 Deployment+Service manifests（k3s 即可）。
runner 仍建议裸 ECS——容器化 FUSE（privileged + /dev/fuse + allow_other）引入的变量大于收益。

## 5. 实验完释放

```bash
aliyun ecs DeleteInstance --InstanceId i-xxx --ForceRelease true
```
