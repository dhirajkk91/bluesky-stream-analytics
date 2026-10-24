# Catalog and SQL access to the raw event files.
resource "aws_glue_catalog_database" "bluesky" {
  name = "bluesky"
}

# One row per line of the raw .jsonl.zst files. Partitions are projected from
# the S3 layout, so new hours are queryable as soon as they are uploaded.
resource "aws_glue_catalog_table" "raw_events" {
  name          = "raw_events"
  database_name = aws_glue_catalog_database.bluesky.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "EXTERNAL"                  = "TRUE"
    "projection.enabled"        = "true"
    "projection.dt.type"        = "date"
    "projection.dt.format"      = "yyyy-MM-dd"
    "projection.dt.range"       = "${var.collection_start_date},NOW"
    "projection.hr.type"        = "integer"
    "projection.hr.range"       = "0,23"
    "projection.hr.digits"      = "2"
    "storage.location.template" = "s3://${aws_s3_bucket.raw.bucket}/raw/events/date=$${dt}/hour=$${hr}/"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  partition_keys {
    name = "hr"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.bucket}/raw/events/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    columns {
      name = "line"
      type = "string"
    }

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    }
  }
}

resource "aws_athena_workgroup" "bluesky" {
  name = var.project

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_bytes

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.raw.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

locals {
  named_queries = {
    "create-events-view" = "views/events.sql"
    "events-per-hour"    = "checks/events_per_hour.sql"
    "missing-minutes"    = "checks/missing_minutes.sql"
    "duplicates"         = "checks/duplicates.sql"
    "event-mix"          = "checks/event_mix.sql"
  }
}

resource "aws_athena_named_query" "saved" {
  for_each = local.named_queries

  name      = each.key
  workgroup = aws_athena_workgroup.bluesky.name
  database  = aws_glue_catalog_database.bluesky.name
  query     = file("${path.module}/../../sql/${each.value}")
}
