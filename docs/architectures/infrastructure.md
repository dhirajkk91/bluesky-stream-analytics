# Infrastructure

## Purpose

Two clouds, each for what it does best within the budget: an always-free Oracle Cloud server runs the collector, the uploader and the Spark job, and AWS stores and queries the data. The AWS side is Terraform in `infra/aws/`; the server's services are unit files in `deploy/systemd/`. The reasons for the split are in [D-008](../decisions/engineering-decisions.md#d-008-collector-server-on-oracle-cloud-data-on-aws).

## Design

```mermaid
flowchart LR
    subgraph Laptop["Laptop"]
        TF["Terraform<br/>infra/aws/"]
        SSH["SSH, key login"]
    end

    subgraph OCI["Oracle Cloud, US Midwest (Chicago)"]
        subgraph VM["VM.Standard.A1.Flex, Ubuntu 24.04"]
            C["bluesky-collect.service"]
            U["bluesky-upload.timer<br/>→ bluesky-upload.service"]
            P["bluesky-process.timer<br/>→ bluesky-process.service"]
            D["/var/lib/bluesky"]
            E["/etc/bluesky/*.env<br/>root only"]
        end
    end

    subgraph AWS["AWS, us-east-1"]
        S3[("S3 bucket<br/>bluesky-stream-analytics-raw-&lt;account&gt;")]
        IAM["IAM users<br/>uploader: PutObject on raw/*<br/>processor: warehouse/ and lake tables"]
        GL["Glue databases<br/>bluesky, bluesky_store"]
        B["Budget<br/>$10/month"]
    end

    J["Jetstream"] --> C
    C --> D
    U --> D
    E -.->|"keys, via systemd"| U
    E -.->|"keys, via systemd"| P
    U -->|"HTTPS"| S3
    P -->|"uploaded/"| D
    P -->|"HTTPS"| S3
    P -->|"commit"| GL
    IAM -.-> U
    IAM -.-> P
    TF -.->|creates| S3
    TF -.->|creates| IAM
    TF -.->|creates| B
    TF -.->|creates| GL
    SSH -.-> VM
```

## The server

| | |
|---|---|
| Provider | Oracle Cloud Always Free, home region US Midwest (Chicago) |
| Shape | `VM.Standard.A1.Flex` (ARM), 2 OCPU, 12 GB memory, 100 GB boot volume |
| OS | Ubuntu 24.04 (aarch64) |
| Account | Pay As You Go, so the server isn't reclaimed as idle. Free within the Always Free limits |
| Network | VCN `bluesky-vcn` (`10.0.0.0/16`) with an internet gateway, a default route `0.0.0.0/0` to it, and a public subnet `10.0.0.0/24`. Only SSH (22) open inbound, key login only. The programs only connect outbound |

| Path | What |
|---|---|
| `/opt/bluesky-stream-analytics` | Git checkout of this repo, with its virtual environment and the Iceberg jars in `.jars/` |
| `/var/lib/bluesky` | Collector data (`events/`, `state/cursor.json`) and files waiting for the Spark job (`uploaded/`) |
| `/etc/bluesky/upload.env` | Bucket name and the uploader's AWS key, `root:root`, mode `600` |
| `/etc/bluesky/process.env` | Bucket name and the processor's AWS key, `root:root`, mode `600` |
| `/etc/systemd/system/bluesky-*` | Copies of `deploy/systemd/` |

| Unit | What it does |
|---|---|
| `bluesky-collect.service` | Runs `collect`. Starts on boot, restarts 5 seconds after a crash, stopped with `SIGTERM` so the current file is finished |
| `bluesky-upload.timer` | Starts `bluesky-upload.service` at :00, :10, :20 … every hour |
| `bluesky-upload.service` | Runs `upload` once. systemd reads the key file as root and passes the values in as environment variables |
| `bluesky-process.timer` | Starts `bluesky-process.service` at 20 past every hour |
| `bluesky-process.service` | Runs the Spark job once, with Java 21. Lower CPU priority than the collector and capped at 6 GB of memory |

All three run as an unprivileged `bluesky` user. systemd keeps the rest of the filesystem read-only to them, so the only place they can write is `/var/lib/bluesky`. Logs go to the systemd journal.

A1 capacity is only needed to start an instance, so the server is rebooted, never stopped. A stopped instance can only start again if the region has free capacity.

## AWS

| Resource | Setup |
|---|---|
| S3 bucket `bluesky-stream-analytics-raw-<account id>` | `us-east-1`, private, public access blocked, encrypted, HTTPS only, incomplete uploads cleaned up after 1 day. Raw files under `raw/` expire after 14 days; Iceberg tables live under `warehouse/` |
| IAM user `bluesky-stream-analytics-uploader` | Only `s3:PutObject` under `raw/`. Its access key is created outside Terraform, so the secret never lands in the state file |
| IAM user `bluesky-stream-analytics-processor` | Reads, writes and deletes under `warehouse/`, reads (never changes) `raw/`, and creates and updates tables in `bluesky_store` only. Key created outside Terraform too |
| Glue database `bluesky_store` | The event store's Iceberg table, located at `warehouse/bluesky_store/` |
| Glue database `bluesky`, table `raw_events` | External table over `raw/events/`, partitions projected from the path. See [query layer](query-layer.md) |
| Athena workgroup `bluesky-stream-analytics` | 10 GB scan limit per query, results in `athena-results/` (expire after 7 days), settings enforced |
| Budget `bluesky-stream-analytics-monthly` | $10 a month, measured before credits. Emails at 80% of actual spend and when the month is forecast to go over |

Terraform state is kept locally and ignored by git.
