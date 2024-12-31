defmodule ExCubicIngestion.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_cubic_ingestion,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      test_coverage: [
        tool: LcovEx
      ],
      dialyzer: [
        ignore_warnings: ".dialyzer.ignore-warnings",
        plt_add_apps: [:mix]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {ExCubicIngestion.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:configparser_ex, "~> 4.0"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      {:ecto, "~> 3.7"},
      {:ecto_sql, "~> 3.7"},
      {:ehmon, git: "https://github.com/mbta/ehmon.git", branch: "master", only: :prod},
      {:ex_aws, "~> 2.2"},
      {:ex_aws_rds, "~> 2.0"},
      {:ex_aws_s3, "~> 2.3"},
      {:hackney, "~> 1.18"},
      {:httpoison, "~> 1.8"},
      {:jason, "~> 1.0"},
      {:lcov_ex, "~> 0.2", only: [:dev, :test], runtime: false},
      {:oban, "~> 2.11"},
      {:postgrex, "~> 0.16"},
      {:ssl_verify_fun, "~> 1.1"},
      {:sweet_xml, "~> 0.7"},
      {:telemetry, "~> 1.0"},
      {:timex, "~> 3.7"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end
end
