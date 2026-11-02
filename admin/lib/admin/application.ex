defmodule Admin.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Admin.Repo, []},
      {Plug.Cowboy,
       scheme: :https,
       options: [
         certfile: "priv/cert/selfsigned.pem",
         keyfile: "priv/cert/selfsigned_key.pem",
         otp_app: :admin,
         port: 4000
       ],
       plug: Admin.Router}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Admin.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
