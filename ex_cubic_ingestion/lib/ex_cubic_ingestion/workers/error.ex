defmodule ExCubicIngestion.Workers.Error do
  @moduledoc """
  Oban Worker for copying loads from the 'Incoming' bucket to the 'Error' one.
  Also, deletes the load from the 'Incoming' bucket.
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

    # configs
    incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)
    error_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_error)
    error_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_error)

    # get load info
    load_rec = CubicLoad.get!(load_rec_id)

    source_key_root = Path.rootname(load_rec.s3_key, ".csv.gz")
    destination_key_root = construct_destination_key_root(load_rec)

    data_source_key = "#{incoming_prefix}#{source_key_root}.csv.gz"
    data_destination_key = "#{error_prefix}#{destination_key_root}.csv.gz"

    # move data file to destination
    {move_data_status, move_data_response} =
      ExAws.Helpers.move(
        lib_ex_aws,
        incoming_bucket,
        data_source_key,
        error_bucket,
        data_destination_key
      )

    # if ODS, also move the metadata file to destination
    {move_metadata_status, move_metadata_response} =
      if String.starts_with?(load_rec.s3_key, "cubic/ods_qlik/") do
        metadata_source_key = "#{incoming_prefix}#{source_key_root}.dfm"

        metadata_destination_key = "#{error_prefix}#{destination_key_root}.dfm"

        ExAws.Helpers.move(
          lib_ex_aws,
          incoming_bucket,
          metadata_source_key,
          error_bucket,
          metadata_destination_key
        )
      else
        {:ok, "No metadata file available"}
      end

    if move_data_status == :ok and move_metadata_status == :ok do
      CubicLoad.update(load_rec, %{status: "errored"})

      :ok
    else
      {:error,
       "Data File: #{move_data_status}: #{inspect(move_data_response)}, Metadata File: #{move_metadata_status}: #{inspect(move_metadata_response)}"}
    end
  end

  @doc """
  Determine the  destination root (path, excluding extension) for the 'error' bucket
  """
  @spec construct_destination_key_root(CubicLoad.t()) :: String.t()
  def construct_destination_key_root(load_rec) do
    # for ODS, the destination will include the timestamp to prevent from overwriting
    if String.starts_with?(load_rec.s3_key, "cubic/ods_qlik/") do
      Enum.join([
        Path.dirname(load_rec.s3_key),
        "/timestamp=#{Calendar.strftime(load_rec.s3_modified, "%Y%m%dT%H%M%SZ")}/",
        Path.basename(load_rec.s3_key, ".csv.gz")
      ])
    else
      Enum.join([
        Path.dirname(load_rec.s3_key),
        '/#{Path.basename(load_rec.s3_key, ".csv.gz")}'
      ])
    end
  end
end
