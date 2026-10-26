output "raw_bucket" {
  description = "Bucket for raw event files."
  value       = aws_s3_bucket.raw.bucket
}

output "uploader_user" {
  description = "IAM user for the collector server."
  value       = aws_iam_user.uploader.name
}

output "athena_workgroup" {
  description = "Workgroup for querying the raw events."
  value       = aws_athena_workgroup.bluesky.name
}

output "processor_user" {
  description = "IAM user for the event store job on the collector server."
  value       = aws_iam_user.processor.name
}

output "warehouse" {
  description = "Iceberg warehouse location for the event store, projections and marts."
  value       = "s3://${aws_s3_bucket.raw.bucket}/${local.warehouse_prefix}"
}
