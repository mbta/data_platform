defmodule ExCubicIngestion.S3Scan do
  @moduledoc """
  Helper function to scan an S3 bucket, managing the continuation token.
  """
  @type prefix :: %{
          prefix: String.t()
        }
  @type object :: %{
          e_tag: String.t(),
          key: String.t(),
          last_modified: String.t(),
          owner: String.t() | nil,
          size: String.t(),
          storage_class: String.t()
        }

  @type scan_result :: prefix | object

  @type list_objects_v2_opts :: [
          {:delimiter, binary()}
          | {:prefix, binary()}
          | {:encoding_type, binary()}
          | {:max_keys, 0..1000}
          | {:stream_prefixes, boolean()}
          | {:fetch_owner, boolean()}
          | {:start_after, binary()}
          | {:lib_ex_aws, module()}
        ]

  @doc """
  Scan an S3 bucket (using ListObjectsV2).

  If a continuation token is returned, the stream will continue until the end.
  """
  @spec list_objects_v2(binary) :: Enumerable.t()
  @spec list_objects_v2(binary, list_objects_v2_opts) :: Enumerable.t()
  def list_objects_v2(bucket, opts \\ []) do
    {lib_ex_aws, opts} = Keyword.pop(opts, :lib_ex_aws, ExAws)

    Stream.resource(
      fn -> {lib_ex_aws, bucket, opts, nil} end,
      &list_objects_v2_with_token/1,
      fn _ignored -> :ok end
    )
  end

  defp list_objects_v2_with_token({_lib_ex_aws, _bucket, _opts, ""}) do
    {:halt, :ok}
  end

  defp list_objects_v2_with_token({lib_ex_aws, bucket, opts, token}) do
    send_opts =
      if token do
        [{:continuation_token, token} | opts]
      else
        opts
      end

    result = lib_ex_aws.request!(ExAws.S3.list_objects_v2(bucket, send_opts))

    elements = result.body.common_prefixes ++ result.body.contents

    acc = {lib_ex_aws, bucket, opts, result.body.next_continuation_token}

    {elements, acc}
  end
end
