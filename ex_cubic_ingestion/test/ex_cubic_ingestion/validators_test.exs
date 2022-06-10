defmodule ExCubicIngestion.ValidatorsTest do
  use ExUnit.Case, async: true

  alias ExCubicIngestion.Validators

  describe "valid_iso_date?/2" do
    test "string passed in is valid per our requirement" do
      assert Validators.valid_iso_date?("2022-01-01")
    end

    test "string passed in is not valid per our requirement" do
      refute Validators.valid_iso_date?("20220101")
    end
  end

  describe "valid_iso_datetime?/2" do
    test "string passed in is valid per our requirement" do
      assert Validators.valid_iso_datetime?("2022-01-01T00:00:00.000000")
    end

    test "string passed in is not valid per our requirement" do
      refute Validators.valid_iso_datetime?("20220101 00:00:00")
    end
  end

  describe "valid_dmap_dataset_url?/1" do
    test "is a valid url" do
      assert Validators.valid_dmap_dataset_url?("https://dmap/sample")
    end

    test "url is not using https" do
      refute Validators.valid_dmap_dataset_url?("http://dmap/sample")
    end

    test "url path is not valid" do
      refute Validators.valid_dmap_dataset_url?("http://dmap")

      refute Validators.valid_dmap_dataset_url?("http://dmap/")
    end
  end

  describe "map_has_keys?/2" do
    test "map has the keys specified" do
      assert Validators.map_has_keys?(
               %{
                 "id" => "1",
                 "key" => "value",
                 "other_key" => "other_value"
               },
               ["id", "key"]
             )
    end

    test "not all keys exist in map" do
      refute Validators.map_has_keys?(
               %{
                 "id" => "1"
               },
               ["id", "key"]
             )
    end
  end

  describe "valid_s3_object?/1" do
    test "is valid object" do
      object = %{
        # ...
        key: "cubic/ods_qlik/EDW.SAMPLE/LOAD1.csv",
        size: "123"
      }

      assert Validators.valid_s3_object?(object)
    end

    test "is valid even if size is 0" do
      object = %{
        # ...
        key: "cubic/ods_qlik/EDW.SAMPLE/LOAD1.csv",
        size: "0"
      }

      assert Validators.valid_s3_object?(object)
    end

    test "invalid if missing key" do
      object = %{
        # ...
        size: "123"
      }

      refute Validators.valid_s3_object?(object)
    end

    test "invalid if missing size" do
      object = %{
        # ...
        key: "cubic/ods_qlik/EDW.SAMPLE/LOAD1.csv"
      }

      refute Validators.valid_s3_object?(object)
    end

    test "invalid if key ends with '/'" do
      object = %{
        # ...
        key: "cubic/ods_qlik/EDW.SAMPLE/",
        size: "123"
      }

      refute Validators.valid_s3_object?(object)
    end
  end
end
