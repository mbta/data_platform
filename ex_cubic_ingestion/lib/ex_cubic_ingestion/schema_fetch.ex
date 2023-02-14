defmodule ExCubicIngestion.SchemaFetch do
  @moduledoc """
  Defines fetchers for the various providers of schemas.
  """

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.Schema.CubicTable

  require Logger

  @spec get_cubic_ods_qlik_columns(module(), CubicLoad.t()) :: list()
  @doc """
  Using the load's S3 key, we identify the Cubic provided schema and extract the columns.
  """
  def get_cubic_ods_qlik_columns(lib_ex_aws, load_rec) do
    incoming_bucket = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_incoming)
    incoming_prefix = Application.fetch_env!(:ex_cubic_ingestion, :s3_bucket_prefix_incoming)

    s3_key_root = Path.rootname(load_rec.s3_key, ".csv.gz")

    incoming_bucket
    |> ExAws.S3.get_object("#{incoming_prefix}#{s3_key_root}.dfm")
    |> lib_ex_aws.request()
    |> handle_get_cubic_ods_columns()
  end

  @spec get_glue_columns(module(), CubicTable.t(), CubicLoad.t()) :: list()
  @doc """
  With the table's name we can pull the Glue table from the data catalog and extract the columns.
  """
  def get_glue_columns(lib_ex_aws, table_rec, load_rec) do
    glue_database_incoming = Application.fetch_env!(:ex_cubic_ingestion, :glue_database_incoming)

    table_name =
      if load_rec.s3_key |> Path.dirname() |> String.ends_with?("__ct") do
        "#{table_rec.name}__ct"
      else
        table_rec.name
      end

    glue_database_incoming
    |> ExAws.Glue.get_table(table_name)
    |> lib_ex_aws.request!()
    |> map_fetch_glue_columns()
    |> Enum.map(&Map.fetch!(&1, "Name"))
  end

  @spec handle_get_cubic_ods_columns({:ok, map()} | {:error, any()}) :: list()
  defp handle_get_cubic_ods_columns({:ok, response}) do
    response
    |> Map.fetch!(:body)
    |> Jason.decode!()
    |> map_fetch_cubic_ods_columns()
    |> Enum.map(&String.downcase(Map.fetch!(&1, "name")))
  end

  defp handle_get_cubic_ods_columns({:error, response}) do
    Logger.error(
      "[ex_cubic_ingestion] [schema_fetch] Unable to fetch schema: #{inspect(response)}"
    )

    []
  end

  @spec map_fetch_cubic_ods_columns(map()) :: list()
  defp map_fetch_cubic_ods_columns(%{"dataInfo" => %{"columns" => columns}}) do
    columns
  end

  @spec map_fetch_glue_columns(map()) :: list()
  defp map_fetch_glue_columns(%{"Table" => %{"StorageDescriptor" => %{"Columns" => columns}}}) do
    columns
  end
end
