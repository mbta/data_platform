defmodule Admin.MixProject do
  use Mix.Project

  def project do
    [
      app: :admin,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Admin.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:ecto_sql, "~> 3.11"},
      {:ex_aws, "~> 2.5"},
      {:ex_aws_rds, "~> 2.0"},
      {:jason, "~> 1.4"},
      {:lcov_ex, "~> 0.3.3", only: [:dev, :test], runtime: false},
      {:plug, "~> 1.15"},
      {:plug_cowboy, "~> 2.6"},
      {:postgrex, "~> 0.17.4"},
      {:ssl_verify_fun, "~> 1.1"},
      {:x509, "~> 0.8.8", only: :dev, runtime: false}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
