
# Data Platform (Terraform)

In general, CTD's policy for documenting infrastucture is through Terraform and the [devops](https://github.com/mbta/devops) repo.

This area exists for management of infrastructure as it relates to **local development** of the Data Platform. As you develop resources, you might want to test out different setups. Files in this folder are purposely ignored, so as not interfere with other developers' resources.

### Setup

In root directory:
```
asdf add-plugin terraform
asdf install
```

In this folder:
```
terraform init
```

### Examples

Here are some of examples of things you might want to deploy for yourself.

* Glue Databases
```
# glue.tf
resource "aws_glue_catalog_database" "ggjura_incoming" {
  name = "ggjura_incoming"
}
```

* Glue Data Catalog Tables
```
# glue.tf
```

* Glue Jobs
```
# glue.tf

```
