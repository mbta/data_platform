defmodule ExAws.Helpers do
  @moduledoc """
  Central place for useful functions that do ExAws work.
  """

  @doc """
  Performs checks, before copying the source object to a destination object
  and deleting the source object. First is the check for if they copy has
  already occurred. And the second check is for existance source object to
  prevent running a copy/delete on a non-existent source.
  """
  @spec copy_and_delete(module(), String.t(), String.t(), String.t(), String.t()) :: :ok | :error
  def copy_and_delete(lib_ex_aws, source_bucket, source_key, destination_bucket, destination_key) do
    {source_status, _req_response} =
      lib_ex_aws.request(ExAws.S3.head_object(source_bucket, source_key))

    {destination_status, _req_response} =
      lib_ex_aws.request(ExAws.S3.head_object(destination_bucket, destination_key))

    cond do
      # if source and destination exist, just delete the source
      source_status == :ok and destination_status == :ok ->
        lib_ex_aws.request!(ExAws.S3.delete_object(source_bucket, source_key))

        :ok

      # already copied
      source_status == :error and destination_status == :ok ->
        :ok

      # no source object, so error
      source_status == :error ->
        :error

      # do the copy and delete
      true ->
        lib_ex_aws.request!(
          ExAws.S3.put_object_copy(destination_bucket, destination_key, source_bucket, source_key)
        )

        lib_ex_aws.request!(ExAws.S3.delete_object(source_bucket, source_key))

        :ok
    end
  end
end
