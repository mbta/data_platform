defmodule ExCubicIngestion.Schema.CubicDmapDatesetTest do
  use ExCubicIngestion.DataCase, async: true

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed

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
          "last_updated" => "2022-05-18T15:12:24.897363" # 3 hours later
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
          last_updated_at: ~U[2022-05-18T15:12:24.897363Z]
        },
        %{
          start_date: ~D[2022-05-18],
          end_date: ~D[2022-05-18],
          last_updated_at: ~U[2022-05-19T12:12:24.897363Z]
        }
      ]

      actual =
        datasets
        |> CubicDmapDataset.upsert_many_from_datasets(dmap_feed)
        |> Enum.sort_by(& &1.id)
        |> Enum.map(
          &%{
            start_date: &1.start_date,
            end_date: &1.end_date,
            last_updated_at: &1.last_updated_at
          }
        )

      assert expected == actual
    end
  end
end
