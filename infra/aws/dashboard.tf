locals {
  dashboard_prefix = "dashboard"
}

# The public dashboard is a static page on GitHub Pages. Each night its workflow
# copies the export from the bucket, with short-lived credentials that GitHub's
# OIDC provider vouches for, so no AWS key is kept in GitHub.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

# Only this repository's Pages deployments can take the role.
data "aws_iam_policy_document" "dashboard_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repository}:environment:github-pages"]
    }
  }
}

resource "aws_iam_role" "dashboard" {
  name               = "${var.project}-dashboard"
  assume_role_policy = data.aws_iam_policy_document.dashboard_trust.json
}

data "aws_iam_policy_document" "dashboard" {
  statement {
    sid       = "ReadDashboardExport"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/${local.dashboard_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "dashboard" {
  name   = "read-dashboard-export"
  role   = aws_iam_role.dashboard.id
  policy = data.aws_iam_policy_document.dashboard.json
}
