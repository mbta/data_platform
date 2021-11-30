

# @TODO move (all?) slowly to devops repo




# bucket for data ingestions
data "aws_s3_bucket" "data_lake_ingest" {
  bucket = "mbta-ctd-data-lake-ingest"
}
resource "aws_s3_bucket_policy" "data_lake_ingest" {
  bucket = aws_s3_bucket.data_lake_ingest.id

  policy = jsonencode({
    Version = "2012-10-17"
    Id      = ""
    Statement = [
      {
        Sid       = "IPAllow"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = "${aws_s3_bucket.data_lake_ingest.arn}/cubic_qlik/*",
        Condition = {
          NotIpAddress = {
            "aws:SourceIp" = ".../32" # @todo
          }
        }
      },
    ]
  })
}

# bucket for data that is staged, and ready for next step
data "aws_s3_bucket" "data_lake_archive" {
  bucket = "mbta-ctd-data-lake-archive"
}

# bucket for data that is staged, and ready for next step
data "aws_s3_bucket" "data_lake_stage" {
  bucket = "mbta-ctd-data-lake-stage"
}










  
# iam role for vendor to use to upload their data
resource "aws_iam_role" "test_role" {
  name = "test_role"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"  
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })

  tags = {
    tag-key = "tag-value"
  }
}

# iam role for importing data using lambda and glue job
resource "aws_iam_role" "test_role" {
  name = "test_role"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })

  tags = {
    tag-key = "tag-value"
  }
}

# glue job for importing the initial load
resource "aws_glue_job" "example" {
  name     = "example"
  role_arn = aws_iam_role.example.arn

  command {
    script_location = "s3://${aws_s3_bucket.example.bucket}/example.py"
  }
}

# glue job for importing the capture data change (CDC) load
resource "aws_glue_job" "example" {
  name     = "example"
  role_arn = aws_iam_role.example.arn

  command {
    script_location = "s3://${aws_s3_bucket.example.bucket}/example.py"
  }
}

