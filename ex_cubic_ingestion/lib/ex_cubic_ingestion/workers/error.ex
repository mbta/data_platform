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

    source_key = "#{incoming_prefix}#{load_rec.s3_key}"

    destination_key = "#{error_prefix}#{construct_destination_key(load_rec)}"

    case ExAws.Helpers.move(
           lib_ex_aws,
           incoming_bucket,
           source_key,
           error_bucket,
           destination_key
         ) do
      {:ok, _req_response} ->
        CubicLoad.update(load_rec, %{status: "errored"})

        :ok

      {:error, req_response} ->
        {:error, req_response}
    end
  end

  @doc """
  Determine the  destination key for the 'error' bucket (excluding 'error' prefix)
  """
  @spec construct_destination_key(CubicLoad.t()) :: String.t()
  def construct_destination_key(load_rec) do
    # for ODS, the destination will include the timestamp to prevent from overwriting
    if String.starts_with?(load_rec.s3_key, "cubic/ods_qlik/") do
      Enum.join([
        Path.dirname(load_rec.s3_key),
        "/timestamp=#{Calendar.strftime(load_rec.s3_modified, "%Y%m%dT%H%M%SZ")}/",
        Path.basename(load_rec.s3_key)
      ])
    else
      load_rec.s3_key
    end
  end
end
