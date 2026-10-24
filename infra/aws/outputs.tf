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
