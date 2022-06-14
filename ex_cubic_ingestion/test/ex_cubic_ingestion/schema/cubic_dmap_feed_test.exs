defmodule ExCubicIngestion.Schema.CubicDmapFeedTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicDmapDataset
  alias ExCubicIngestion.Schema.CubicDmapFeed

  describe "get_by!/2" do
    test "getting only items that are not deleted or error" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/transactional/sample1"
        })

      # insert deleted record
      Repo.insert!(%CubicDmapFeed{
        relative_url: "/controlledresearchusersapi/transactional/sample2",
        deleted_at: ~U[2022-01-01 20:50:50Z]
      })

      assert dmap_feed ==
               CubicDmapFeed.get_by!(
                 relative_url: "/controlledresearchusersapi/transactional/sample1"
               )

      assert_raise Ecto.NoResultsError, fn ->
        CubicDmapFeed.get_by!(relative_url: "/controlledresearchusersapi/transactional/sample2")
      end

      assert_raise Ecto.NoResultsError, fn ->
        CubicDmapFeed.get_by!(
          relative_url: "/controlledresearchusersapi/transactional/does_not_exist"
        )
      end
    end
  end

  describe "update_last_updated_for_feed/2" do
    test "no updates are made for empty list of datasets" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample",
          last_updated_at: ~U[2022-05-16 20:49:50.123456Z]
        })

      assert ~U[2022-05-16 20:49:50.123456Z] ==
               CubicDmapFeed.update_last_updated_from_datasets(
                 [],
                 dmap_feed
               ).last_updated_at
    end

    test "update with the latest updated dataset" do
      dmap_feed =
        Repo.insert!(%CubicDmapFeed{
          relative_url: "/controlledresearchusersapi/sample",
          last_updated_at: ~U[2022-05-16 20:49:50.123456Z]
        })

      dmap_dataset_1 =
        Repo.insert!(%CubicDmapDataset{
          feed_id: dmap_feed.id,
          type: "sample",
          identifier: "sample_20220517",
          start_date: ~D[2022-05-17],
          end_date: ~D[2022-05-17],
          last_updated_at: ~U[2022-05-18 12:12:24.897363Z]
        })

      dmap_dataset_2 =
        Repo.insert!(%CubicDmapDataset{
          feed_id: dmap_feed.id,
          type: "sample",
          identifier: "sample_20220518",
          start_date: ~D[2022-05-18],
          end_date: ~D[2022-05-18],
          last_updated_at: ~U[2022-05-19 12:12:24.897363Z]
        })

      assert ~U[2022-05-19 12:12:24.897363Z] ==
               CubicDmapFeed.update_last_updated_from_datasets(
                 [dmap_dataset_1, dmap_dataset_2],
                 dmap_feed
               ).last_updated_at
    end
  end
end
