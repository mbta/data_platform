defmodule :"Elixir.ExCubicOdsIngestion.Repo.Migrations.Drop-is-cdc-column" do
  use Ecto.Migration

  def change do
    alter table("cubic_ods_loads") do
      remove :is_cdc
    end
  end
end
