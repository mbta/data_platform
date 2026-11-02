defmodule ExCubicIngestion.Schema.CubicDmapDatetimeTest do
  use ExUnit.Case, async: true

  alias ExCubicIngestion.Schema.CubicDmapDataset

  describe "iso_extended_to_datetime/1" do
    test "treats timestamps without an offset as UTC and preserves microseconds" do
      assert CubicDmapDataset.iso_extended_to_datetime("2022-05-18T12:12:24.897363") ==
               ~U[2022-05-18T12:12:24.897363Z]
    end

    test "accepts explicit UTC timestamps" do
      assert CubicDmapDataset.iso_extended_to_datetime("2022-05-18T12:12:24Z") ==
               ~U[2022-05-18T12:12:24Z]
    end

    test "normalizes positive and negative offsets to UTC" do
      assert CubicDmapDataset.iso_extended_to_datetime("2022-05-18T12:12:24.897363+02:00") ==
               ~U[2022-05-18T10:12:24.897363Z]

      assert CubicDmapDataset.iso_extended_to_datetime("2022-05-18T12:12:24-05:00") ==
               ~U[2022-05-18T17:12:24Z]
    end

    test "raises on invalid timestamps" do
      assert_raise ArgumentError, fn ->
        CubicDmapDataset.iso_extended_to_datetime("not a timestamp")
      end
    end
  end
end
