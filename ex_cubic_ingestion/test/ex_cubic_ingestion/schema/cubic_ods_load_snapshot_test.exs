defmodule ExCubicIngestion.Schema.CubicOdsLoadSnapshotTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicOdsLoadSnapshot

  describe "formatted/1" do
    test "datetime is properly formatted even" do
      assert "20230101T010203Z" ==
               CubicOdsLoadSnapshot.formatted(
                 DateTime.new!(~D[2023-01-01], ~T[01:02:03], "Etc/UTC")
               )

      # with sub-second indication
      assert "20230101T010203Z" ==
               CubicOdsLoadSnapshot.formatted(
                 DateTime.new!(~D[2023-01-01], ~T[01:02:03.456], "Etc/UTC")
               )
    end
  end
end
