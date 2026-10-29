variable "project" {
  description = "Name used as a prefix for every resource."
  type        = string
  default     = "bluesky-stream-analytics"
}

variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "budget_email" {
  description = "Email address that receives budget alerts."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly budget limit in USD, before credits."
  type        = number
  default     = 10
}

variable "collection_start_date" {
  description = "First date with raw event files (yyyy-MM-dd)."
  type        = string
  default     = "2026-10-24"
}

variable "athena_scan_limit_bytes" {
  description = "Maximum bytes a single Athena query may scan."
  type        = number
  default     = 10737418240
}

variable "raw_retention_days" {
  description = "Days raw event files are kept in S3."
  type        = number
  default     = 14
}

variable "github_repository" {
  description = "GitHub repository whose Pages workflow may read the dashboard export (owner/name)."
  type        = string
  default     = "dhirajkk91/bluesky-stream-analytics"
}
