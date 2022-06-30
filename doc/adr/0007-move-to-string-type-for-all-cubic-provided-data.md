
# 7. Standardizing on 'string' for all initial Glue Catalog Table column types

Date: 2022-06-09

## Status

Draft

## Context

We have been using the AWS Glue Crawler to determine the columns and types of data that are coming from Cubic for both ODS and DMAP data feeds. In most cases, the Crawler will sample the data and make the determination. This has worked pretty well in quickly getting the column names, but not that great for figuring out the types of these columns. For some tables with a small amount of data, such as `cubic_ods_qlik__edw_benefit_status_dimension`, the sample size is consitent and many times will not change, resulting in the crawler making an accurate assumption about the types. But for much larger tables, the data in the sample may not reflect data further along in the file, hence the crawler making an incorrect determination.

Because we are now processing more data from Cubic, these mislabels are creating issues when this data is queried in Athena. Here are examples of such issues:

```
HIVE_BAD_DATA: Field sys_created_by's type BINARY in parquet file 
s3://mbta-ctd-dataplatform-dev-springboard/cubic/ods_qlik/EDW.SVN_U_FS_RPIR_CODE_RT_CAUSE_ID
/snapshot=20220608T234623Z/identifier=LOAD00000001.csv.gz
/part-00001-bfe93dfd-7c3b-42d2-b88d-4577a4f1bda2.c000.snappy.parquet 
is incompatible with type bigint defined in table schema
```

```
HIVE_CANNOT_OPEN_SPLIT: Error opening Hive split 
s3://mbta-ctd-dataplatform-dev-springboard/cubic/ods_qlik/EDW.ABP_TAP
/snapshot=20220607T160603Z/identifier=LOAD00000001.csv.gz
/part-00015-7cc52de2-9ca3-4ede-b2b7-004cea96a414.c000.snappy.parquet 
(offset=0, length=2904265): org.apache.parquet.io.GroupColumnIO cannot be cast 
to org.apache.parquet.io.PrimitiveColumnIO
```

Some stakeholders will be pulling data right out of the Springboard bucket. We need to make sure that it's always available. We obviously can't guarantee this, but we can try by defaulting on the `string` type for all columns, and only swaying from the default when we are sure that we can determine the type and have the proper fallbacks in place. Ideally, a `string` type for a column of data in Incoming bucket with `1`, `2`, `3` as values can be an `bigint` type in the Springboard. These conversions do make Athena querying more efficient, so we should strive to apply them as much as possible, but for unknown data we should default to using the `string` type.

## Assumptions

AWS Glue is still a fairly new technology and it seems to be constantly evolving. [This article](https://aws.amazon.com/premiumsupport/knowledge-center/glue-crawler-detect-schema/) describes how the Crawler works, but based on experience we believe that the Crawler is possibly making mistakes when it comes to determining the type, especially wrongly attributing the `bigint` type.

Unfortunately Athena and Glue are not open source and therefore we cannot make an absolute assertion that this is occuring. We are only relying on experience with this new technology (in Glue's case) to make this assumption.

In addition, we are also assuming that initially standardizing on the type, specifically the `string` type, is a good approach to dealing with data querying issues in Athena. Almost all types have a `toString()` implementation, so relying on it feels safe. It also seems to be a fallback for the Glue Crawler, as we tend to only see `string`  and `'bigint'` types surface from its process.

## Implemetation

As indicated, our solution will involve initially standardizing the type for all data coming in. We will choose the `string` type, as it will be the most inclusive and is usually supported in a standard way across many technologies. This will minimize any issues with being able to write the data to parquet and reading it out.

Our implementation will be as follows:

1. Update Glue table Terraform modules to identify all incoming data columns as `string` types.

2. Add an additional table to the Terraform modules for the Springboard destination, which will be marked with the 'raw_' prefix (ex. `raw_cubic_ods_qlik__edw_sample`), and will have a location that includes the 'raw/' prefix (ex. `s3://springboard-bucket/raw/cubic/ods_qlik/EDW.SAMPLE/...`). This table will be used to query un-processed data with Athena. (Note: The existing Springboard destination table can be used only once some types have been determined and the data is being ingested and processed.)

3. In order to keep track that the loads being ingested are processed or not, we will mark the table and load records with the `is_raw` flag. This will reflect the state of the load in the Springboard bucket.

4. Make adjustments to the Glue Job to allow 'raw' loads to be stored with a different prefix in the Springboard bucket.

5. Reprocess the existing data:

    - Ask Cubic to restart Qlik and send new snapshots for all ODS tables to the Incoming bucket and on to the Springboard. We can then clean up the Springboard bucket and/or delete older partitions from Athena.
    - Reprocess the data ourselves utilizing the Archive bucket and overwrite the Springboard data. We will then reload partitions in Athena, which might not be necessary.

## Moving Forward

1. All new tables added for ingestion will be marked with `is_raw: true` initially.

2. Once some or all column types have been determined for the table, we will switch the `is_raw` flag to `false`.

## Consequences

* Even though initially standardizing on the `string` type will create the stability we are seeking, we have to be conscious of the fact that we are not really moving the needle on cleaning up the data and truly preparing it for further ingestion. Currently, we don't have visibility on how this data will be ingested beyond being queried in Athena and possibly downloaded there, so it feels premature to make optimization beyond this standard. However, with this architecture and base we can easily expand scope and achieve results as noted above.
* We will be creating additional tables in the Glue Databases in the Terraform modules (see [this](https://github.com/mbta/devops/tree/master/terraform/modules/app-dataplatform/glue-data-catalog/cubic_ods_qlik__table) and [this](https://github.com/mbta/devops/tree/master/terraform/modules/app-dataplatform/glue-data-catalog/cubic_dmap__table)). Although cost-wise this will not have much impact, we might have tables in Glue that are not being utilized, hence creating some clutter.
