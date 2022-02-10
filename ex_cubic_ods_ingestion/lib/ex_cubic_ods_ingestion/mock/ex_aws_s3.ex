defmodule ExCubicOdsIngestion.Mock.ExAws.S3 do
  @moduledoc """
  ExCubicOdsIngestion.Mock.ExAws.S3 @todo
  """

  @spec list_objects_v2(bucket :: binary) :: ExAws.Operation.S3.t()
  @spec list_objects_v2(bucket :: binary, list()) :: ExAws.Operation.S3.t()
  @params [
    :delimiter,
    :prefix,
    :encoding_type,
    :max_keys,
    :continuation_token,
    :fetch_owner,
    :start_after
  ]
  def list_objects_v2(bucket, opts \\ []) do
    %{
      body: %{
        contents: [
          %{
            e_tag: "\"abc123\"",
            key: "vendor/SAMPLE/LOAD1.csv",
            last_modified: "2022-02-08T20:49:50.000Z",
            owner: nil,
            size: "197",
            storage_class: "STANDARD"
          },
          %{
            e_tag: "\"def123\"",
            key: "vendor/SAMPLE/LOAD2.csv",
            last_modified: "2022-02-08T20:49:50.000Z",
            owner: nil,
            size: "123",
            storage_class: "STANDARD"
          }
        ],
        next_continuation_token: ""
      }
    }

    # ::::: original implementation :::::
    #
    # params =
    #   opts
    #   |> format_and_take(@params)
    #   |> Map.put("list-type", 2)

    # request(:get, bucket, "/", [params: params, headers: opts[:headers]],
    #   stream_builder: &ExAws.S3.Lazy.stream_objects!(bucket, opts, &1),
    #   parser: &ExAws.S3.Parsers.parse_list_objects/1
    # )
  end
end
