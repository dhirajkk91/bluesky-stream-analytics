# The event store: every Jetstream event exactly once, as an Iceberg table
# written by the DuckDB job on the server through Glue's Iceberg REST endpoint.
# The job creates its own tables; this database and the processor's permissions
# are all it needs. The processor can read raw files but never change them.
locals {
  warehouse_prefix = "warehouse"
  store_database   = "bluesky_store"
}

resource "aws_glue_catalog_database" "store" {
  name         = local.store_database
  location_uri = "s3://${aws_s3_bucket.raw.bucket}/${local.warehouse_prefix}/${local.store_database}"
}

resource "aws_iam_user" "processor" {
  name = "${var.project}-processor"
  path = "/service/"
}

data "aws_iam_policy_document" "processor" {
  statement {
    sid       = "ListWarehouseAndRaw"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.warehouse_prefix}/*", "raw/*"]
    }
  }

  # For backfills and rebuilds from the raw files S3 still keeps.
  statement {
    sid       = "ReadRawFiles"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/*"]
  }

  statement {
    sid = "ReadWriteWarehouse"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    resources = ["${aws_s3_bucket.raw.arn}/${local.warehouse_prefix}/*"]
  }

  # The Iceberg REST endpoint needs the catalog itself, and lists namespaces
  # (database names only) when the job attaches it.
  statement {
    sid = "UseIcebergRestCatalog"
    actions = [
      "glue:GetCatalog",
      "glue:GetDatabase",
      "glue:GetDatabases",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/*",
    ]
  }

  statement {
    sid = "ManageLakeTables"
    actions = [
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${local.store_database}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${local.store_database}/*",
    ]
  }
}

# The marts: tables dbt builds in Athena from the event store.
resource "aws_glue_catalog_database" "marts" {
  name         = "bluesky_marts"
  location_uri = "s3://${aws_s3_bucket.raw.bucket}/${local.warehouse_prefix}/bluesky_marts"
}

data "aws_iam_policy_document" "processor_marts" {
  # dbt creates, replaces and drops its own tables and views, and cleans up old
  # table versions in Glue, only in the marts database.
  statement {
    sid = "ManageMartTables"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
      "glue:GetTableVersions",
      "glue:DeleteTableVersion",
      "glue:BatchDeleteTableVersion",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.marts.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.marts.name}/*",
    ]
  }

  # After the build, the dashboard's numbers are exported for the public page.
  statement {
    sid       = "WriteDashboardExport"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/${local.dashboard_prefix}/*"]
  }
}

# A managed policy rather than another inline one: IAM caps all of a user's
# inline policies together at 2,048 characters, and the processor's are full.
resource "aws_iam_policy" "processor_marts" {
  name   = "${var.project}-build-marts"
  policy = data.aws_iam_policy_document.processor_marts.json
}

resource "aws_iam_user_policy_attachment" "processor_marts" {
  user       = aws_iam_user.processor.name
  policy_arn = aws_iam_policy.processor_marts.arn
}

# Compaction runs its SQL in Athena: the queries, and the results Athena writes.
data "aws_iam_policy_document" "processor_athena" {
  statement {
    sid = "RunCompactionQueries"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.bluesky.arn]
  }

  statement {
    sid       = "FindResultsBucket"
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn]
  }

  statement {
    sid       = "ListQueryResults"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["athena-results/*"]
    }
  }

  statement {
    sid = "WriteQueryResults"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
    ]
    resources = ["${aws_s3_bucket.raw.arn}/athena-results/*"]
  }
}

resource "aws_iam_user_policy" "processor_athena" {
  name   = "run-compaction-in-athena"
  user   = aws_iam_user.processor.name
  policy = data.aws_iam_policy_document.processor_athena.json
}

resource "aws_iam_user_policy" "processor" {
  name   = "manage-lake-tables"
  user   = aws_iam_user.processor.name
  policy = data.aws_iam_policy_document.processor.json
}
