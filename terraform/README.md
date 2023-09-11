
# Data Platform (Terraform)

In general, CTD's policy for documenting infrastucture is through Terraform and the [devops](https://github.com/mbta/devops) repo.

This area exists for management of infrastructure as it relates to **local development** of the Data Platform. As you develop resources, you might want to test out different setups. Files in this folder are purposely ignored, so as not interfere with other developers' resources.

### Setup

In root directory:
```
asdf plugin-add terraform
asdf install
```

In this folder:
```
terraform init
```

### Examples

Here are some of examples of things you might want to deploy for yourself. Replace `{username}` and `{aws_account_id}` appropriately.

* Glue Databases
```
# glue.tf
resource "aws_glue_catalog_database" "{username}_incoming" {
  name = "{username}_incoming"
}

resource "aws_glue_catalog_database" "{username}_springboard" {
  name = "{username}_springboard"
}
```

* Glue Jobs
```
# glue.tf
resource "aws_glue_job" "cubic_ingestion_ingest_incoming" {
  name         = "{username}_cubic_ingestion_ingest_incoming"
  role_arn     = "arn:aws:iam::{aws_account_id}:role/dataplatform-local-glue"
  glue_version = "3.0"

  # From AWS Validation: To use Worker Type G.1X, minimum allowed value of Number of Workers is 2.
  number_of_workers = 2
  worker_type       = "G.1X"

  execution_property {
    max_concurrent_runs = 10
  }

  command {
    script_location = "s3://mbta-ctd-dataplatform-local/{username}/operations/glue_jobs/cubic_ingestion/ingest_incoming.py"
  }
}
```

* Glue Data Catalog Tables
```
# glue.tf

# NOTE: Before adding the following, copy all the contents of
# https://github.com/mbta/devops/tree/master/terraform/modules/app-dataplatform/glue-data-catalog
# into a new 'glue_data_catalog' folder within this directory. You can delete
# any tables you don't need, or deploy them all.

module "glue_data_catalog" {
  source = "./glue_data_catalog"

  incoming_database_name    = aws_glue_catalog_database.{username}_incoming.name
  springboard_database_name = aws_glue_catalog_database.{username}_springboard.name
  incoming_bucket           = "mbta-ctd-dataplatform-local/{username}/incoming"
  springboard_bucket        = "mbta-ctd-dataplatform-local/{username}/springboard"
}
```
