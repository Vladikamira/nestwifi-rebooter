# Nest Wifi: Exporter and Planned Reboot

Two separate Go binaries, with no Python, `grpcurl`, or external `protoc` at
runtime. Google APIs are unofficial: firmware/cloud changes may require
adaptation. **By default, the rebooter does not modify settings or send a reboot.**

## What Is Implemented

### Exporter

Automatic discovery (or an optional static list), concurrency-limited
background polls, `/metrics`, `/healthz`, and `/readyz`. Prometheus HTTP scrapes
read an in-memory snapshot rather than making network requests to devices.

- Router: `GET http://IP/api/v1/status`.
- Nest Wifi Point: `GET https://IP:8443/setup/eureka_info`, Cast V2 TCP:8009.
- `nestwifi_device_up{device,ip,kind}`, `nestwifi_uptime_seconds`,
  `nestwifi_boot_timestamp_seconds`,
  `nestwifi_device_info{model,firmware,role,update_status}` — Router role
  `root`/`leaf`, update status (`idle`, …).
- `nestwifi_backhaul_wired` — 1 if the point is wired to the network, 0 if
  wireless (omitted on root Router).
- `nestwifi_device_value{path="wan.online"|"wan.ethernetLink"|…}` — WAN metrics
  reported by each Router.
- `nestwifi_scrape_duration_seconds`, `nestwifi_scrape_errors_total`,
  `nestwifi_last_success_timestamp_seconds`.
- `nestwifi_cast_up`, `nestwifi_cast_volume_ratio`, `nestwifi_cast_muted`.
- `nestwifi_device_value{path,...}`: numeric/boolean fields from an explicit
  allowlist: Wi-Fi RSSI/noise/state, online/ethernet, LAN link, OTA
  progress/required, night mode/DND/volume/brightness, setup state, and others.
  Full list: `internal/exporter/exporter.go:NumericValues`. Missing fields are
  not fabricated.
- String diagnostic fields, SSIDs, MACs, client IPs, certificates, and tokens
  **are not exported**. Arbitrary JSON is not converted into metrics to avoid
  leaks and unbounded cardinality.
- If Cast does not respond, API metrics and uptime continue to be collected;
  `nestwifi_cast_up=0`. A failure on one point does not block the others.

If a local GET works without a LAT, the exporter does not contact the cloud. If
the API requires a token, it obtains one automatically. When using only public
diagnostic endpoints, a master token is not required; manifests use a shared
Secret for both services.

#### Cloud Metrics

Every `exporter.cloud_interval` (default 1 min, `0` to disable), the exporter
reads the Google Wifi API (`/v2/groups`, `/status`, `/realtimeMetrics`,
`/stations`; GET only). `device` labels match the local metrics.

| Metric | What It Shows |
|---|---|
| `nestwifi_mesh_speed_score{device,ip}` | point mesh link quality as in the Home app (`OK` = 50); omitted for root |
| `nestwifi_mesh_speed_rating{device,ip,rating}` | same as text (`OK`, …) |
| `nestwifi_ap_online{device,ip,hardware,root}` | point status according to the cloud |
| `nestwifi_ap_last_seen_timestamp_seconds` | when the cloud last heard from the point |
| `nestwifi_wan_online` | internet status |
| `nestwifi_traffic_bits_per_second{direction}` | current traffic across the whole network |
| `nestwifi_clients{device,connection,band}` | active clients per point, wired/2.4/5 GHz |
| `nestwifi_stations{state}` | known clients: connected/disconnected |
| `nestwifi_cloud_up`, `..._last_success_timestamp_seconds`, `..._errors_total` | cloud poll status |

This is Google's rating, not RSSI in dBm: live RSSI is not returned by either the
local or cloud API. Clients are exported **only as aggregates** — without names,
MACs, or IPs. Local tokens and serial numbers from the API response are not even
deserialized. On cloud failure, the last values are retained and
`nestwifi_cloud_up=0`; local metrics do not depend on the cloud. If
`rebooter.group_id` is empty, the account's single group is used.

### Rebooter

A single continuously running process with a five-field cron schedule and an IANA
timezone:

1. Runs a fresh discovery: **all** HomeGraph Wifi devices are found on the LAN
   exactly once, their LAN IPs match the point IPs of group `group_id` according
   to the cloud, and all are reachable.
   A Point is matched by name, model, and `cloud_device_id`; a Router by
   `hardwareId`.
2. Saves original night mode, DND, schedule, brightness, volume, mute, and
   boot time to the PVC. Performs an atomic `fsync` of state before each change.
3. Enables **24/7 night mode, `volume=0`, and DND** on each Point without
   changing brightness. Reads settings back; after `quiet_settle`, verifies all
   Points again, including actual Cast volume 0.
4. Only after confirmation sends **one** cloud REST reboot for the entire group.
   There is no gRPC fallback or automatic reboot retry.
5. Waits for reachability and a new boot time on **every** device, then waits
   for `settle`.
6. Refreshes the LAT via HomeGraph and restores original settings, including
   original mute — not an unconditional unmute. Verifies the result.

Night mode was tested with two silent local reboots on Kitchen Wifi, firmware
`1.56.539812`. Plain Cast mute **does not suppress** the startup chime. A cloud
reboot of the entire mesh network with this flow has not yet been tested; lack
of chime should not be assumed guaranteed on every model/firmware.

Failure protection:

- Quiet/preflight error → reboot is not sent; modified settings are rolled back.
- Indeterminate cloud POST result → no reboot retry; boot times are checked.
- Wait timeout → restore reachable Points, continue restoring the rest every
  `recovery_interval`; new reboots are blocked until finished.
- After a process restart, an unfinished operation is recovered first; **a reboot
  is never resent**, even if the process crashed between writing intent and the
  HTTP request. Skipping a reboot is deliberately preferred over duplicating it.
- Recovery also runs without `-enable-reboot`. This is required so turning off
  the schedule does not leave a Point stuck in night mode.
- Single instance, `Recreate` strategy, PVC with POSIX `flock`, `rename`,
  `fsync`. The exclusive file lock is held for the entire lifetime of the
  process. Do not run separate state PVCs against the same network: there is no
  cross-cluster/distributed lock here.
- If the master token is revoked or the cloud is unreachable, night mode
  restoration may be delayed. It cannot be guaranteed under complete
  unreachability of devices/Google; alerts and a persisted PVC are required.

The rebooter's `/readyz` returns 503 while an operation is unfinished;
`/healthz` stays 200. There is intentionally no HTTP endpoint to trigger a
reboot; control is only via MQTT (below) and the schedule.

Additionally, the rebooter supports (including via buttons in Home Assistant):

- **quiet reboot of a single point** — the same procedure: night mode → reboot →
  verify new boot → new LAT → restore settings; full inventory is checked;
- **speedtest** (~30 s; via button and after `speedtest_delay` following a
  successful reboot); metrics
  `nestwifi_speedtest_{download,upload}_bits_per_second`,
  `..._timestamp_seconds`;
- **status light brightness** for each point (0–100%);
- **internet pause and priority** — only for clients in `managed_clients`.

`min_interval` (default 1 h) rate-limits repeated reboots for **the same target**
(whole network or a specific point), including presses in HA. Rejection shows up
as `last_result=too_soon`.

**Preflight retries.** The Google cloud is occasionally unavailable or rate-limits
requests, and a Point sometimes answers the LAN scan slowly. A *scheduled* run
whose preflight fails — before anything is changed — is retried
`preflight_retries` times (default 6) every `preflight_retry_interval` (default
5 min). Failures after the speakers were quieted are never retried. A button
press in HA is a single attempt.

**Why a reboot did not happen** is logged and published: `last_result`
(`preflight_failed`, `too_soon`, `quiet_failed`, `failed`, …) plus `last_error`
in plain words, e.g. `inventory incomplete (4/5 matched): Kitchen Wifi: not found
on the LAN` or `GetHomeGraph failed: Unavailable: …`. Network causes such as
`i/o timeout` or `no such host` are included; tokens and request URLs are not.
In HA it is the "Last reboot problem" diagnostic sensor.

Discovery re-probes, with a 5 s timeout, addresses where a Wifi device answered
before but missed the fast 1.5 s scan. Telemetry uses a cached HomeGraph (refreshed
at most every 10 minutes, and kept when a refresh fails); the rebooter always
fetches it fresh before changing anything.

## Home Assistant via MQTT

Requires an MQTT broker and the MQTT integration in HA (**HA 2024.11+**: whole-device
discovery is used). Set `mqtt.url` in the config and the devices appear
automatically:

- **Nest Wifi** (network): internet status, ↓/↑ traffic, connected/known
  clients; from the rebooter — "Quiet reboot" and "Run speed test" buttons,
  "Scheduled reboot" switch, last reboot result and timestamp, next run,
  "Reboot in progress", "Settings restore pending", speedtest results; for
  `managed_clients` — "Internet …" switch and "Prioritize … 1h" button.
- **5 points** (Living Room, Office, Storage, Kitchen, Kids room): online, last
  boot time, mesh link quality, 2.4/5 GHz/wired clients, wired uplink, role,
  firmware, update status; "Quiet restart" button; "Status light" `light` with
  brightness.

Devices are tied to the point's cloud ID, not IP or name. A point visible to the
cloud but not on the LAN stays in HA: `Online` via cloud, `Local API` = off.
Exporter and rebooter publish their availability (LWT) separately, so buttons
become unavailable if the rebooter is not running. After an HA restart
(`homeassistant/status` = `online` message), discovery is republished.

Topics (`topic_prefix: nestwifi`): state — `nestwifi/ap/<point>/state`,
`nestwifi/network/state`, `nestwifi/rebooter/state`; commands —
`nestwifi/rebooter/reboot`, `…/speedtest`, `…/schedule/set`,
`…/ap/<point>/reboot`, `…/ap/<point>/light/set`, `…/client/<client>/set`.

Command safety:

- a command with the retain flag left on the broker is **ignored** — otherwise it
  would trigger on every reconnect;
- a reboot only runs when started with `-enable-reboot`; without it, the button
  performs a dry run (`last_result=dry_run`);
- all rebooter checks and `min_interval` apply to buttons too; while an
  operation is in progress, a new one is rejected; no duplicate cloud commands;
- **restrict access to the broker**: anyone who can publish to `nestwifi/#` can
  reboot the network. Use username/password and ACLs.

Password comes from `MQTT_PASSWORD_FILE` (in manifests, an optional Secret
`nestwifi-mqtt`, key `password`; without it, anonymous connection):

```sh
kubectl -n nestwifi create secret generic nestwifi-mqtt \
  --from-file=password=/secure/path/mqtt-password
```

**TLS** is built in: `url: ssl://host:8883`, `tls.ca_file` (for example,
`/mqtt/ca.crt` from the same Secret), and client `cert_file`/`key_file` if
needed. `insecure_skip_verify` is for debugging only.

Broker connection errors are logged with retries (5 s … 1 min); after the first
connection, reconnection is automatic. The default MQTT client ID includes the pod
name (two clients with one ID keep disconnecting each other), and each process
re-asserts `online` every minute. The exporter Deployment uses the `Recreate`
strategy, so only one instance publishes and polls the cloud at a time.

### Google Quota

Google rate-limits requests (`HTTP 429`). By default, the exporter polls the
cloud every 2 minutes (`exporter.cloud_interval`, at least 1 min) and backs off
up to 30 min on errors. The rebooter updates status lights and clients every 5
minutes. Do not run extra copies of the services with the same account.

## Authentication and Token Refresh

Inputs for both processes:

| Variable | Meaning |
|---|---|
| `GOOGLE_EMAIL` | Google account owning the Wifi network |
| `GOOGLE_MASTER_TOKEN_FILE` | Path to file containing `aas_et/...`, preferably a Secret volume |
| `GOOGLE_MASTER_TOKEN` | Alternative to file; file takes precedence |
| `GOOGLE_ANDROID_ID` | Default `0123456789abcdef`; use the ID with which the master token was obtained |

Chain: Android OAuth exchange → access token → gRPC HomeGraph → LAT.
Access token is cached for at most 45 minutes (or until stated expiry with a
buffer). HomeGraph is cached for 10 minutes, force-refreshed on local 401, on
every actual discovery scan, and before restoration after a reboot. The master
file is read when requesting an access token; changing it invalidates the access
cache. A projected Secret updates without `subPath`; rotation detection may wait
for the next request/TTL.

All short-lived tokens live only in memory. The master token is not copied into
state or config. The master token itself cannot be renewed automatically: a
revoked token must be obtained again outside the services and the Secret updated.
An active master token is as sensitive as account credentials; restrict Secret
RBAC and enable Kubernetes Secrets encryption at rest.

During discovery, a Point is matched with HomeGraph by local `cloud_device_id`
and the nested HomeGraph identifier (not by name). The corresponding stable
`cloud_id` goes into the operation config and is later used to refresh the LAT.
In static legacy mode, an exact `cloud_name` is allowed, though that is a weaker
binding; `home_id` scopes the search to a home.

## Discovery Without a Device List

Enabled by default in `deploy/k8s/config.yaml`:

```yaml
discovery:
  enabled: true
  cidrs: [192.168.86.0/24]
  interval: 2m
  timeout: 1500ms
  concurrency: 32
```

Individual IPs are not needed. Set only the allowed LAN subnet and the chosen
`rebooter.group_id`; email/master token come from a Secret. mDNS is not
required: only `GET /api/v1/status` on TCP:80 and `GET /setup/eureka_info` on
TCP:8443 are scanned. Nest Wifi Router (MISTRAL)/Google Wifi (GALE) — both as
`root` and as `leaf` (a Router used as a point) — as well as **Nest Wifi Point**
with a speaker are recognized. Regular Nest Audio/Home speakers are ignored.
Only private IPv4 subnets no wider than `/22`, up to 1024 addresses, are
allowed. Scan only your own LAN.

Expected network composition is all Wifi devices in the home from HomeGraph
(type `ROUTER` and model `Nest Wifi point`). Each must be found on the LAN
**exact once**:

- **Point**: local `cloud_device_id` == nested HomeGraph ID
  (DeviceInfo → ProjectInfo, field 2). This is not the external HomeGraph
  `device_id`.
- **Router**: checked for whether it accepts the local token from that HomeGraph
  entry (`GET :8443/api/v1/weave-info`). Router tokens are device-bound: on the
  tested network, each of the three Routers accepted only its own token.
- After matching, a Router is additionally identified by `hardwareId`, so an IP
  change after a reboot will not mix up devices.

Verified on a real network: Living Room (`root`), Office and Storage (`leaf`),
Kitchen and Kids room Point — `complete=true`, 5/5.

Names are not used as proof of identity. An unfound, duplicate, or unidentified
Wifi device → `complete=false`: the rebooter does not change settings or send a
reboot, while the exporter continues serving telemetry. The rebooter also
requires the set of LAN IPs of found devices to match the point IPs of the
selected cloud group (the cloud reports each point's IP). If there are multiple
Wifi networks in one home, set `home_id` or use static mode — otherwise the sets
will not match and reboot will be disallowed.

`rebooter -list-groups` shows each group's points: IP, model (H2D — Router,
H2E — Point), root, and name. That makes it easy to verify the right group is
selected.

`device` labels come from HomeGraph names (`living-room-wifi`, `kitchen-wifi`,
…); temporary `router-<ip>`/`point-<hash>` labels are used before matching.
Series are tracked by a stable key, so an IP or name change does not create
duplicates inside the process.

> On macOS, locally built binaries may get `no route to host` for all LAN
> addresses except the gateway until granted "Local Network" permission. This is
> not a discovery bug; there is no such restriction in a container/cluster.

The exporter repeats discovery every `interval` and keeps previously found
devices **in process memory** so a missing point continues reporting
`device_up=0`. After a restart, this inventory is rebuilt from scratch; also
monitor `nestwifi_discovery_complete`, `..._expected_devices`,
`..._matched_devices`, `..._last_scan_timestamp_seconds`, and
`..._errors_total`. Without the cloud, local telemetry is available, but
inventory completeness is not confirmed and reboot is disallowed.

The rebooter always forces a fresh scan before an operation. The operation's
device set is saved to the PVC; during wait/recovery, addresses may be updated
only for those same stable identifiers. New devices are not added to an
in-progress operation, and original settings are not overwritten.

Read-only view of discovered inventory (tokens are not printed in JSON):

```sh
./bin/exporter -config deploy/k8s/config.yaml -discover
./bin/rebooter -config deploy/k8s/config.yaml -discover
```

If you need static configuration, set `discovery.enabled: false` and specify
the previous `devices` array. Both modes cannot be mixed. The IP set in this
mode must also match the group's point IPs, but devices are bound to HomeGraph
only by exact name (`cloud_name`) — which is weaker than discovery mode.

## Local Build and Safe Verification

```sh
cd services
go test -race ./...
go build -o bin/ ./cmd/exporter ./cmd/rebooter
./bin/exporter -config deploy/k8s/config.yaml
```

For the rebooter, first change `state_dir` to a writable local directory (for
example `.state`) and set email/master via environment or file. Do not put real
tokens into YAML or shell history.

```sh
# List groups only; does not modify devices.
./bin/rebooter -config deploy/k8s/config.yaml -list-groups

# After setting group_id: discovery + preflight, no changes.
./bin/rebooter -config deploy/k8s/config.yaml -once

# DANGEROUS: quiet mode, REAL reboot of the ENTIRE mesh network, restore.
./bin/rebooter -config deploy/k8s/config.yaml -once -enable-reboot

# Continuous process with schedule and reboot enabled.
./bin/rebooter -config deploy/k8s/config.yaml -enable-reboot
```

`-once` with an unfinished state runs **recovery only** and exits. Cron does not
catch up on runs missed during downtime. Time obeys the timezone and DST, so a
run may be skipped or repeated on a clock-change day. One running job will not
run in parallel with another.

## Docker and Kubernetes

From the `services` directory:

```sh
docker build --target exporter -t nestwifi-exporter:local .
docker build --target rebooter -t nestwifi-rebooter:local .
```

For a registry/multi-arch build, use `docker buildx build --platform
linux/amd64,linux/arm64 --target ... -t REGISTRY/IMAGE:TAG --push .`.
Set your image references in `deploy/k8s/workloads.yaml`; for
kind/k3d/minikube, load local images into the cluster using its standard method.

1. Edit `deploy/k8s/config.yaml`: LAN CIDR, timezone, schedule, and — after
   `-list-groups` — group ID. A fixed IP list is not required.
2. Create the namespace and Secret from protected local files (file contents do
   not appear in command arguments):

   ```sh
   kubectl apply -f deploy/k8s/namespace.yaml
   kubectl -n nestwifi create secret generic nestwifi-auth \
     --from-file=email=/secure/path/google-email \
     --from-file=master-token=/secure/path/master-token
   ```

3. Apply the manifests. By default, the schedule runs only a read-only
   preflight.

   ```sh
   kubectl apply -k deploy/k8s
   kubectl -n nestwifi logs deployment/nestwifi-rebooter
   kubectl -n nestwifi exec deployment/nestwifi-rebooter -- \
     /rebooter -config /config/config.yaml -list-groups
   kubectl -n nestwifi port-forward service/nestwifi-exporter 9109:9109
   # In another terminal: curl [http://127.0.0.1:9109/metrics](http://127.0.0.1:9109/metrics)
   ```

4. After verification, add `"-enable-reboot"` to the rebooter `args` and apply
   `kubectl apply -k deploy/k8s` again. ConfigMap changes require a rollout:
   configuration is read once at startup; kustomize changes the ConfigMap name
   hash automatically.
5. If Prometheus Operator is installed, apply `servicemonitor.yaml` and
   `alerts.yaml` after adjusting `release` for your stack. Without the Operator,
   use a standard scrape config for both Services, port 9109, `/metrics`.

Rebooter metrics: `nestwifi_rebooter_enabled`, `..._active`,
`..._restore_pending`, `..._jobs_total{outcome}`, `..._last_attempt_timestamp_seconds`,
`..._last_success_timestamp_seconds`. Auth metrics for both services:
`nestwifi_auth_access_last_refresh_timestamp_seconds`,
`nestwifi_auth_homegraph_last_refresh_timestamp_seconds` (0 if unused).
Process metric counters and timestamps reset on restart; unfinished operation
state is stored separately on the PVC.

### Cluster Network

Pods need outbound access to the LAN: TCP 80 (Router), 8443 and 8009 (Point),
plus DNS and TCP 443 to Google. For a standard CNI with LAN egress/NAT,
`hostNetwork` is not needed. With macvlan or a custom CNI, configure routes
separately. mDNS is not used, so multicast between pod and LAN is not required.

`networkpolicy.yaml` is an optional example for `192.168.86.0/24`, not included
in `kustomization`. Verify DNS, egress NAT/CNI, and subnet before applying.
Local HTTPS/Cast certificates are self-signed: certificate verification is
disabled **only for LAN devices**. Cloud TLS is verified against standard CAs.
Deploy on a trusted network; protection against an active LAN MITM is not
provided by this code.

### Manual Recovery

If `restore_pending=1`: check Point and Google reachability, auth, Secret, and
logs. Do not delete `/state/operation.json`: it holds the only copy of the
original settings. The next recovery attempt will refresh the LAT automatically.
To run `-once` with the same PVC, stop the Deployment first — the exclusive lock
prevents two processes from modifying the network at the same time.

The PVC must remain accessible after a Wifi reboot. It is best to place the
rebooter on a wired node and keep state on storage that does not depend on the
Wi-Fi being rebooted. A network PVC or a cluster connected entirely through this
mesh network can lose access right during recovery.
