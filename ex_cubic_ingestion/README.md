
# (Elixir) Cubic Ingestion

This application ingests data files, provided by Cubic, into our Data Lake. We utilize various AWS services, most importantly S3 for storage and AWS Glue for data transformation and processing.

DMAP and AWS requests use Req with Finch/Mint, matching `delta`. Dataset downloads
disable response decoding and decompression so compressed CSV files are uploaded
unchanged. HTTP failures fail the job rather than uploading an error response.
ISO timestamps are parsed with Elixir's standard library; timestamps without an
offset are treated as UTC.

## Installation

@todo
