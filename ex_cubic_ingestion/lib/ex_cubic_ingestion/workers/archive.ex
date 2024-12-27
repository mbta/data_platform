defmodule ExCubicIngestion.Workers.Archive do
  @moduledoc """
  Oban Worker for copying loads from the 'Incoming' bucket to the 'Archive' one.
  Also, deletes the load from the 'Incoming' bucket.
  """

  use Oban.Worker,
    queue: :archive,
    max_attempts: 1

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot

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
    archive_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_archive)
    archive_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_archive)

    # get load info
    load_rec = CubicLoad.get!(load_rec_id)

    source_key_root = Path.rootname(load_rec.s3_key, ".csv.gz")
    destination_key_root = construct_destination_key_root(load_rec)

    data_source_key = "#{incoming_prefix}#{source_key_root}.csv.gz"
    data_destination_key = "#{archive_prefix}#{destination_key_root}.csv.gz"

    # move data file to destination
    {move_data_status, move_data_response} =
      ExAws.Helpers.move(
        lib_ex_aws,
        incoming_bucket,
        data_source_key,
        archive_bucket,
        data_destination_key
      )

    # if ODS, also move the metadata file to destination
    {move_metadata_status, move_metadata_response} =
      if CubicLoad.ods_load?(load_rec.s3_key) do
        metadata_source_key = "#{incoming_prefix}#{source_key_root}.dfm"

        metadata_destination_key = "#{archive_prefix}#{destination_key_root}.dfm"

        ExAws.Helpers.move(
          lib_ex_aws,
          incoming_bucket,
          metadata_source_key,
          archive_bucket,
          metadata_destination_key
        )
      else
        {:ok, "No metadata file available"}
      end

    if move_data_status == :ok and move_metadata_status == :ok do
      CubicLoad.update(load_rec, %{status: "archived"})

      :ok
    else
      {:error,
       "Data File: #{move_data_status}: #{inspect(move_data_response)}, Metadata File: #{move_metadata_status}: #{inspect(move_metadata_response)}"}
    end
  end

  @doc """
  Determine the  destination root (path, excluding extension) for the 'archive' bucket
  """
  @spec construct_destination_key_root(CubicLoad.t()) :: String.t()
  def construct_destination_key_root(load_rec) do
    # for ODS, the destination will include the snapshot to prevent from overwriting
    if CubicLoad.ods_load?(load_rec.s3_key) do
      ods_load_snapshot_rec = CubicOdsLoadSnapshot.get_latest_by!(load_id: load_rec.id)

      Enum.join([
        Path.dirname(load_rec.s3_key),
        "/snapshot=#{Calendar.strftime(ods_load_snapshot_rec.snapshot, "%Y%m%dT%H%M%SZ")}/",
        Path.basename(load_rec.s3_key, ".csv.gz")
      ])
    else
      Enum.join([
        Path.dirname(load_rec.s3_key),
        "/#{Path.basename(load_rec.s3_key, ".csv.gz")}"
      ])
    end
  end
end
