
resource "aws_iam_user" "uploader" {
  name = "${var.project}-uploader"
  path = "/service/"
}

data "aws_iam_policy_document" "uploader" {
  statement {
    sid       = "PutRawFiles"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/*"]
  }
}

resource "aws_iam_user_policy" "uploader" {
  name   = "put-raw-files"
  user   = aws_iam_user.uploader.name
  policy = data.aws_iam_policy_document.uploader.json
}
