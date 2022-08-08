defmodule ExAws.Helpers do
  @moduledoc """
  Central place for useful functions that do ExAws work.
  """

  @doc """
  Performs checks, before copying the source object to a destination object
  and deleting the source object (moving).
  """
  @spec move(module(), String.t(), String.t(), String.t(), String.t()) ::
          {:ok, term()} | {:error, term()}
  def move(lib_ex_aws, source_bucket, source_key, destination_bucket, destination_key) do
    {source_status, source_req_response} =
      lib_ex_aws.request(ExAws.S3.head_object(source_bucket, source_key))

    {destination_status, _req_response} =
      lib_ex_aws.request(ExAws.S3.head_object(destination_bucket, destination_key))

    cond do
      source_status == :error and destination_status == :ok ->
        {:ok, "Already copied."}

      source_status == :error ->
        {:error, source_req_response}

      destination_status == :ok ->
        lib_ex_aws.request(ExAws.S3.delete_object(source_bucket, source_key))

      # do the move
      true ->
        {copy_status, copy_req_response} =
          lib_ex_aws.request(
            ExAws.S3.put_object_copy(
              destination_bucket,
              destination_key,
              source_bucket,
              source_key
            )
          )

        if copy_status == :ok do
          lib_ex_aws.request(ExAws.S3.delete_object(source_bucket, source_key))
        else
          {:error, copy_req_response}
        end
    end
  end
end
