defmodule Admin.Router do
  use Plug.Router

  import Ecto.Query

  require Logger

  alias Admin.Repo
  alias Admin.Schema.CubicTable

  @template_dir "lib/admin/templates"

  plug(Plug.SSL, rewrite_on: [:x_forwarded_proto, :x_forwarded_host, :x_forwarded_port])

  plug(Plug.Parsers,
    pass: ["text/*"],
    parsers: [:multipart, :json],
    json_decoder: Jason
  )

  plug(:match)
  plug(:dispatch)

  get "/" do
    render(conn, "index.html")
  end

  get "/_json" do
    render_json(conn, %{})
  end

  get "/_health" do
    send_resp(conn, 200, "Healthy!")
  end

  get "/test" do
    tables = Repo.all(from(table in CubicTable, where: is_nil(table.deleted_at)))

    Logger.info("[admin] #{length(tables)}")

    render(conn, "index.html")
  end

  get "/admin" do
    Logger.info("[admin] Protected")

    render(conn, "index.html")
  end

  match _ do
    send_resp(conn, 404, "Not Found")
  end

  # private
  defp render(%{status: status} = conn, template, assigns \\ []) do
    body =
      @template_dir
      |> Path.join(template)
      |> EEx.eval_file(assigns)

    send_resp(conn, status || 200, body)
  end

  defp render_json(%{status: status} = conn, data) do
    body = Jason.encode!(data)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status || 200, body)
  end
end
