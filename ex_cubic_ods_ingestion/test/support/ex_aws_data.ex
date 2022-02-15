defmodule MockExAws.Data do
  @moduledoc """
  MockExAws.Data @todo
  """

  def load_objects do
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
