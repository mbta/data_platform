defmodule ExCubicOdsIngestion.ProcessIncomingTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.ProcessIncoming
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad

  # setup server for use throughout tests
  setup do
    Application.put_env(:ex_cubic_ods_ingestion, :lib_ex_aws, MockExAws)
    Application.put_env(:ex_cubic_ods_ingestion, :lib_ex_aws_s3, MockExAws.S3)

    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)

    # start a supervisor
    server = start_supervised!(ProcessIncoming)

    %{server: server}
  end

  describe "status/0" do
    test "running state", %{server: server} do
      assert ProcessIncoming.status(server) == :running
    end
  end

  describe "load_objects_list/3" do
    test "getting objects for test prefix" do
      assert [MockExAws.Data.get(), ""] ==
           ProcessIncoming.load_objects_list("cubic_ods_qlik_test/", "")
    end

    test "getting objects for non-existing prefix" do
      assert [[], ""] ==
           ProcessIncoming.load_objects_list("does_not_exist/", "")
    end

    # @todo test for a non-empty continuation token
  end

  describe "load_recs_list/1" do
    test "getting records just added by providing the first load object in list" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.get())

      assert new_load_recs == ProcessIncoming.load_recs_list(List.first(MockExAws.Data.get()))
    end

    test "getting the last record by providing the last load object in list" do
      {:ok, new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.get())

      assert [List.last(new_load_recs)] == ProcessIncoming.load_recs_list(List.last(MockExAws.Data.get()))
    end

    test "getting no records by providing a load object not in db" do
      {:ok, _new_load_recs} = CubicOdsLoad.insert_from_objects(MockExAws.Data.get())

      assert [] == ProcessIncoming.load_recs_list(%{
        e_tag: "\"ghi789\"",
        key: "not/in/db.csv",
        last_modified: "2022-02-08T21:49:50.000Z",
        owner: nil,
        size: "197",
        storage_class: "STANDARD"
      })
    end

    test "getting no records by NOT providing load object" do
      assert [] == ProcessIncoming.load_recs_list(nil)
    end

    # @todo test for improper load object map
  end

  describe "not_added/2" do
    test "object NOT found in database records" do
      load_object = List.first(MockExAws.Data.get())
      load_recs = [%CubicOdsLoad{
        s3_key: "key/not/found.csv",
        s3_modified: ~U[2022-02-08 20:49:50Z]
      }]

      assert ProcessIncoming.not_added(load_object, load_recs)
    end

    test "object found in database records" do
      load_object = List.first(MockExAws.Data.get())
      load_recs = [%CubicOdsLoad{
        s3_key: "vendor/SAMPLE/LOAD1.csv",
        s3_modified: ~U[2022-02-08 20:49:50Z]
      }]

      assert not ProcessIncoming.not_added(load_object, load_recs)
    end
  end
end

defmodule MockExAws do
  @moduledoc """
  MockExAws @todo
  """

  @spec request!(ExAws.Operation.t(), keyword) :: term
  def request!(op, _config_overrides \\ []) do
    Logger.info(op)

    # ::::: original implementation :::::
    #
    # case request(op, config_overrides) do
    #   {:ok, result} ->
    #     result

    #   error ->
    #     raise ExAws.Error, """
    #     ExAws Request Error!
    #     #{inspect(error)}
    #     """
    # end
  end
end

defmodule MockExAws.Data do
  @moduledoc """
  MockExAws.Data @todo
  """

  def get do
    [
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
        last_modified: "2022-02-08T20:50:50.000Z",
        owner: nil,
        size: "123",
        storage_class: "STANDARD"
      }
    ]
  end
end

# defmodule MockExAws.S3 do
#   @moduledoc """
#   MockExAws.S3 @todo
#   """

#   @spec list_objects_v2(bucket :: binary) :: ExAws.Operation.S3.t()
#   @spec list_objects_v2(bucket :: binary, list()) :: ExAws.Operation.S3.t()
#   # @params [
#   #   :delimiter,
#   #   :prefix,
#   #   :encoding_type,
#   #   :max_keys,
#   #   :continuation_token,
#   #   :fetch_owner,
#   #   :start_after
#   # ]
#   def list_objects_v2(_bucket, opts \\ []) do
#     incoming_prefix = Application.fetch_env!(:ex_cubic_ods_ingestion, :s3_prefix_incoming)

#     if opts[:prefix] == "#{incoming_prefix}cubic_ods_qlik_test/" do
#       %{
#         body: %{
#           contents: MockExAws.Data.get(),
#           next_continuation_token: ""
#         }
#       }
#     else
#       %{
#         body: %{
#           contents: [],
#           next_continuation_token: ""
#         }
#       }
#     end

#     # ::::: original implementation :::::
#     #
#     # params =
#     #   opts
#     #   |> format_and_take(@params)
#     #   |> Map.put("list-type", 2)

#     # request(:get, bucket, "/", [params: params, headers: opts[:headers]],
#     #   stream_builder: &ExAws.S3.Lazy.stream_objects!(bucket, opts, &1),
#     #   parser: &ExAws.S3.Parsers.parse_list_objects/1
#     # )
#   end
# end
