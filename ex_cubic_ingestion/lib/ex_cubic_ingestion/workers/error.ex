defmodule ExCubicIngestion.Workers.Error do
  @moduledoc """
  Workers.Error module.
  """

  use Oban.Worker,
    queue: :error,
    max_attempts: 1

  alias ExCubicIngestion.Schema.CubicLoad

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

    # @todo consider moving the code below into a module for both archive and error jobs
    # configs
    incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)
    error_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_error)
    error_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_error)

    # get load info
    load_rec = CubicLoad.get!(load_rec_id)

    source_key = "#{incoming_prefix}#{load_rec.s3_key}"

    destination_key =
      Enum.join([
        error_prefix,
        Path.dirname(load_rec.s3_key),
        "/timestamp=#{Calendar.strftime(load_rec.s3_modified, "%Y%m%dT%H%M%SZ")}/",
        Path.basename(load_rec.s3_key)
      ])

    # copy load file to error bucket
    lib_ex_aws.request!(
      ExAws.S3.put_object_copy(error_bucket, destination_key, incoming_bucket, source_key)
    )

    # delete load file from incoming bucket
    lib_ex_aws.request!(ExAws.S3.delete_object(incoming_bucket, source_key))

    CubicLoad.update(load_rec, %{status: "errored"})

    :ok
  end
end
