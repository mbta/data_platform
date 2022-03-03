defmodule ExCubicOdsIngestion.Workers.Archive do
  @moduledoc """
  Workers.Archive module.
  """

  use Oban.Worker,
    queue: :ingest,
    max_attempts: 1

  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  @impl Oban.Worker
  def perform(%{args: args} = _job) do
    # get info passed into args
    %{"load_rec_id" => load_rec_id} = args

    # allow for ex_aws module to be passed in as a string, since Oban will need to
    # serialize args to JSON. defaulted to library module.
    lib_ex_aws =
      case args do
        %{"lib_ex_aws" => mod_str} -> Module.safe_concat([mod_str])
        _args_lib_ex_aws -> ExAws
      end

    # configs
    incoming_bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_incoming)
    archive_bucket = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_archive)
    archive_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_bucket_prefix_archive)

    # get load info
    load_rec = CubicOdsLoad.get!(load_rec_id)

    source_key = "#{incoming_prefix}#{load_rec.s3_key}"

    destination_key =
      Enum.join([
        archive_prefix,
        Path.dirname(load_rec.s3_key),
        "/snapshot=#{Calendar.strftime(load_rec.snapshot, "%Y%m%d%H%M%S")}/",
        Path.basename(load_rec.s3_key)
      ])

    # copy load file to error bucket
    lib_ex_aws.request!(
      ExAws.S3.put_object_copy(archive_bucket, destination_key, incoming_bucket, source_key)
    )

    # delete load file from incoming bucket
    lib_ex_aws.request!(ExAws.S3.delete_object(incoming_bucket, source_key))

    CubicOdsLoad.update(load_rec, %{status: "archived"})

    :ok
  end
end
