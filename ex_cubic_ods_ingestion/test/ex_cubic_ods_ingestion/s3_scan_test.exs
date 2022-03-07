defmodule ExCubicOdsIngestion.S3ScanTest do
  use ExUnit.Case, async: true

  alias __MODULE__.FakeAws
  alias ExCubicOdsIngestion.S3Scan

  describe "list_objects_v2/2" do
    test "single page" do
      stream = S3Scan.list_objects_v2("bucket", prefix: "prefix/", lib_ex_aws: FakeAws)

      assert [
               %{prefix: "prefix/one/"},
               %{key: "prefix/file.txt"}
             ] = Enum.into(stream, [])
    end

    test "multiple pages" do
      stream = S3Scan.list_objects_v2("bucket", prefix: "big_prefix/", lib_ex_aws: FakeAws)

      assert [
               %{prefix: "big_prefix/one/"},
               %{key: "big_prefix/file_one.txt"},
               %{prefix: "big_prefix/two/"},
               %{key: "big_prefix/file_two.txt"}
             ] = Enum.into(stream, [])
    end

    test "error" do
      assert_raise ExAws.Error, fn ->
        Stream.run(S3Scan.list_objects_v2("bucket_does_not_exist", lib_ex_aws: FakeAws))
      end
    end
  end

  defmodule FakeAws do
    @spec request!(ExAws.Operation.t()) :: term()
    def request!(%{http_method: :get, params: %{"prefix" => "prefix/"}}) do
      %{
        body: %{
          common_prefixes: [
            %{prefix: "prefix/one/"}
          ],
          contents: [
            %{key: "prefix/file.txt"}
          ],
          next_continuation_token: ""
        }
      }
    end

    def request!(%{
          http_method: :get,
          params: %{"prefix" => "big_prefix/", "continuation-token" => "token"}
        }) do
      %{
        body: %{
          common_prefixes: [
            %{prefix: "big_prefix/two/"}
          ],
          contents: [
            %{key: "big_prefix/file_two.txt"}
          ],
          next_continuation_token: ""
        }
      }
    end

    def request!(%{http_method: :get, params: %{"prefix" => "big_prefix/"}}) do
      %{
        body: %{
          common_prefixes: [
            %{prefix: "big_prefix/one/"}
          ],
          contents: [
            %{key: "big_prefix/file_one.txt"}
          ],
          next_continuation_token: "token"
        }
      }
    end

    def request!(r) do
      raise ExAws.Error, "failed request: #{inspect(r)}"
    end
  end
end
