defmodule ExCubicIngestion.Schema.CubicOdsLoadSnapshotTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot

  describe "formatted/1" do
    test "formats DateTime to URL-friendly string" do
      datetime = ~U[2023-12-06T15:30:45Z]

      assert CubicOdsLoadSnapshot.formatted(datetime) == "20231206T153045Z"
    end
  end
end
