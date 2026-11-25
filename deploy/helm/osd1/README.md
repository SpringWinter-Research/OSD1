# OSD1

One process owns one SQLite file. The chart runs that process as user 10001, stores the file at `/data/osd1.db`, and keeps the volume after a restart.

Simple mode is one replica. A second replica is a second empty database, not a copy of the first. The chart rejects `replicaCount` other than 1 unless Raft is on.

Raft mode runs one process per pod. Each pod has its own volume. Pod 0 bootstraps. The other pods join it. The first `raft.voters` pods vote. The default is 3. Further pods join as learners. Quorum is a majority of the voters. The process binds `0.0.0.0` and advertises `<pod>.<release>-raft.<namespace>.svc.cluster.local`, so a new pod IP is still the same peer address. That needs a server that reads `OSD1_RAFT_ADVERTISE` and `OSD1_NODE_ID_FROM_HOSTNAME`. The published tag `2026-11-26` does not include that server yet. Build the image from this tree before `raft.enabled`. Simple mode runs on `2026-11-26`.

Do not scale a Raft release below `raft.voters`. A voter that disappears stays in the quorum until something runs `OSD1_RAFT_REMOVE`. This chart does not remove members.

## Install

```bash
helm install osd1 deploy/helm/osd1 --set auth.token=dev-token
```

```bash
helm install osd1 deploy/helm/osd1 \
  --set auth.token=dev-token \
  --set raft.enabled=true \
  --set raft.secret=raft-secret \
  --set replicaCount=3
```

The client Service is `<release>.<namespace>.svc:<port>`. `/query`, `/raw`, and `/mcp` use the bearer token. `/health`, `/metrics`, and the explorer do not.

```bash
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://osd1.default.svc:8080/accounts/a/d1/database/b/query
```

Set `auth.existingSecret` to skip the generated Secret. The key name is `auth.secretKey` (`token`). For Raft, put the shared secret in `raft.secret`, or set `raft.existingSecret` and `raft.secretKey` (`raft-secret`).

## Probes and disruption

Startup, readiness, and liveness all read `GET /health`. HTTP 200 means the process is up. In Raft mode `ready` can be false while the status stays 200. A node that is leaving returns 503.

The PodDisruptionBudget sets `maxUnavailable: 1`. One voter can be down. Two voters of three still commit.

The pod runs as uid 10001 and gid 10001, with `fsGroup: 10001`. The database file is mode 0600. The mounted volume is group-writable because Kubernetes applies `fsGroup` that way.

## Litestream

Leave `litestream.enabled` false until the bucket exists. The sidecar is `litestream/litestream:0.5`. It uses `scripts/litestream-leader.sh` from this chart. Simple mode replicates continuously. Raft mode replicates only while `osd1_raft_role` is 1, so only the leader writes the bucket.

```bash
helm install osd1 deploy/helm/osd1 \
  --set auth.token=dev-token \
  --set litestream.enabled=true \
  --set litestream.bucket=osd1 \
  --set litestream.endpoint=http://minio:9000 \
  --set litestream.forcePathStyle=true \
  --set litestream.accessKeyId=minioadmin \
  --set litestream.secretAccessKey=minioadmin
```

`litestream.existingSecret` replaces the generated Secret. The keys are `access-key-id` and `secret-access-key`. Restore steps are in `docs/backup-restore.md`.

## Metrics

`GET /metrics` is unauthenticated. The Service carries `prometheus.io/scrape` when `metrics.annotations` is true. `metrics.serviceMonitor.enabled` and `metrics.prometheusRule.enabled` add a ServiceMonitor and the `Osd1ReplicaStale` rule. The rule matches `deploy/prometheus-alerts.yml`.

`networkPolicy.enabled` allows port 8080 from pods in the same namespace, and the Raft port only from these pods. Egress stays open so Litestream can reach S3 and pods can resolve DNS.

## Values

| Value | Default | Purpose |
| --- | --- | --- |
| `replicaCount` | `1` | Pods. Simple mode allows only 1 |
| `image.repository` | `public.ecr.aws/c5l0h3s1/osd1` | Image |
| `image.tag` | `2026-11-26` | Tag. Raft mode needs the advertise support in this chart |
| `auth.token` | empty | Bearer token. Required unless `auth.existingSecret` is set |
| `persistence.size` | `10Gi` | Volume for `/data` |
| `persistence.storageClass` | empty | Empty uses the cluster default |
| `raft.enabled` | `false` | Three or more pods, one file each |
| `raft.voters` | `3` | Odd, and at least 3. `replicaCount` must be at least this |
| `raft.secret` | empty | Shared Raft secret |
| `litestream.enabled` | `false` | S3 sidecar |
| `config.servedByColo` | empty | `OSD1_SERVED_BY_COLO` when set |
| `config.databaseId` | empty | `OSD1_DATABASE_ID` when set |
| `clusterDomain` | `cluster.local` | Suffix on the Raft DNS names |
