defmodule ExCubicOdsIngestion.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_cubic_ods_ingestion,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [
        tool: LcovEx
      ],
      dialyzer: [
        ignore_warnings: ".dialyzer.ignore-warnings"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {ExCubicOdsIngestion.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:configparser_ex, "~> 4.0"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      {:ecto, "~> 3.7"},
      {:ecto_sql, "~> 3.7"},
      {:ex_aws, "~> 2.2"},
      {:ex_aws_rds, "~> 2.0"},
      {:ex_aws_s3, "~> 2.3"},
      {:hackney, "~> 1.18"},
      {:jason, "~> 1.0"},
      {:lcov_ex, "~> 0.2", only: [:dev, :test], runtime: false},
      {:postgrex, "~> 0.16"},
      {:sweet_xml, "~> 0.7"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
