defmodule ExCubicIngestion.Schema.CubicDmapDatesetTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed

  describe "valid_dataset?/1" do
    test "with valid dataset" do
      dataset = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      assert CubicDmapDataset.valid_dataset?(dataset)
    end

    test "with invalid datasets" do
      dataset_missing_field = %{
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_start_date = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-45",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_end_date = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022:05:17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_last_updated = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022:05:18T12:12:24.897363"
      }

      dataset_invalid_url_wrong_scheme = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "file://mbtaqadmapdatalake.blob.core.windows.net/sample",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_url_empty_path = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_invalid_url_invalid_path = %{
        "id" => "sample",
        "dataset_id" => "sample_20220517",
        "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/",
        "start_date" => "2022-05-17",
        "end_date" => "2022-05-17",
        "last_updated" => "2022-05-18T12:12:24.897363"
      }

      dataset_empty = %{}

      refute CubicDmapDataset.valid_dataset?(dataset_missing_field) ||
               CubicDmapDataset.valid_dataset?(dataset_invalid_start_date) ||
               CubicDmapDataset.valid_dataset?(dataset_invalid_end_date) ||
               CubicDmapDataset.valid_dataset?(dataset_invalid_last_updated) ||
               CubicDmapDataset.valid_dataset?(dataset_invalid_url_wrong_scheme) ||
               CubicDmapDataset.valid_dataset?(dataset_invalid_url_empty_path) ||
               CubicDmapDataset.valid_dataset?(dataset_invalid_url_invalid_path) ||
               CubicDmapDataset.valid_dataset?(dataset_empty)
    end
  end

  describe "upsert_many_from_datasets/2" do
    test "updating an existing dataset record and inserting another" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/transactional/sample"
        })

      Repo.insert!(%CubicDmapDataset{
        feed_id: dmap_feed.id,
        type: "sample",
        identifier: "sample_20220517",
        start_date: ~D[2022-05-17],
        end_date: ~D[2022-05-17],
        last_updated_at: ~U[2022-05-18T12:12:24.897363Z]
      })

      datasets = [
        %{
          "id" => "sample",
          "dataset_id" => "sample_20220517",
          "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample/abc123",
          "start_date" => "2022-05-17",
          "end_date" => "2022-05-17",
          # 3 hours later
          "last_updated" => "2022-05-18T15:12:24.897363"
        },
        %{
          "id" => "sample",
          "dataset_id" => "sample_20220518",
          "url" => "https://mbtaqadmapdatalake.blob.core.windows.net/sample/def456",
          "start_date" => "2022-05-18",
          "end_date" => "2022-05-18",
          "last_updated" => "2022-05-19T12:12:24.897363"
        }
      ]

      expected = [
        %{
          start_date: ~D[2022-05-17],
          end_date: ~D[2022-05-17],
          last_updated_at: ~U[2022-05-18T15:12:24.897363Z],
          url: "https://mbtaqadmapdatalake.blob.core.windows.net/sample/abc123"
        },
        %{
          start_date: ~D[2022-05-18],
          end_date: ~D[2022-05-18],
          last_updated_at: ~U[2022-05-19T12:12:24.897363Z],
          url: "https://mbtaqadmapdatalake.blob.core.windows.net/sample/def456"
        }
      ]

      actual =
        datasets
        |> CubicDmapDataset.upsert_many_from_datasets(dmap_feed)
        |> Enum.map(fn {rec, url} ->
          %{
            start_date: rec.start_date,
            end_date: rec.end_date,
            last_updated_at: rec.last_updated_at,
            url: url
          }
        end)

      assert expected == actual
    end
  end
end
