# The event store: every Jetstream event exactly once, as an Iceberg table
# written by the Spark job on the server. The job creates its own tables; this
# database and the processor's permissions are all it needs. The processor can
# read raw files but never change them.
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

  statement {
    sid = "ManageLakeTables"
    actions = [
      "glue:GetDatabase",
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

resource "aws_iam_user_policy" "processor" {
  name   = "manage-lake-tables"
  user   = aws_iam_user.processor.name
  policy = data.aws_iam_policy_document.processor.json
}
