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

    source_key = "#{incoming_prefix}#{load_rec.s3_key}"

    destination_key = "#{archive_prefix}#{construct_destination_key(load_rec)}"

    {move_status, move_req_response} =
      ExAws.Helpers.move(
        lib_ex_aws,
        incoming_bucket,
        source_key,
        archive_bucket,
        destination_key
      )

    if move_status == :ok do
      CubicLoad.update(load_rec, %{status: "archived"})

      :ok
    else
      {:error, move_req_response}
    end
  end

  @doc """
  Determine the  destination key for the 'archive' bucket (excluding 'archive' prefix)
  """
  @spec construct_destination_key(CubicLoad.t()) :: String.t()
  def construct_destination_key(load_rec) do
    # for ODS, the destination will include the snapshot to prevent from overwriting
    if String.starts_with?(load_rec.s3_key, "cubic/ods_qlik/") do
      ods_load_rec = CubicOdsLoadSnapshot.get_by!(load_id: load_rec.id)

      Enum.join([
        Path.dirname(load_rec.s3_key),
        '/snapshot=#{Calendar.strftime(ods_load_rec.snapshot, "%Y%m%dT%H%M%SZ")}/',
        Path.basename(load_rec.s3_key)
      ])
    else
      load_rec.s3_key
    end
  end
end
