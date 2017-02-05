defmodule Beanstix.Mixfile do
  use Mix.Project

  def project do
    [
      app: :beanstix,
      version: "0.1.0",
      elixir: "~> 1.4",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:connection, "~> 1.0"},
      {:credo, "~> 0.6.0-rc2", only: [:dev, :test]},
      {:dialyxir, "~> 0.4", only: [:dev]},
      {:benchfella, "~> 0.3", only: [:bench]},
      {:elixir_talk, "~> 1.1", only: [:bench]},
    ]
  end

end
