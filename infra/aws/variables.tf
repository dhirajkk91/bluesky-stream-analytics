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
