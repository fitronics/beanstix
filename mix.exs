defmodule Beanstix.Mixfile do
  use Mix.Project

  def project do
    [
      app: :beanstix,
      version: "0.1.0",
      elixir: "~> 1.4",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps(),
      # Docs
      name: "Beanstix",
      source_url: "https://github.com/nicksanders/beanstix",
      docs: [main: "Beanstix"]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Beanstix.Application, []}
    ]
  end

  defp deps do
    [
      {:shackle, "~> 0.5.0"},
      {:ex_doc, "~> 0.14", only: :dev, runtime: false},
      {:credo, "~> 0.7.2", only: [:dev, :test]},
      {:dialyxir, "~> 0.4", only: [:dev]},
      {:benchfella, "~> 0.3", only: [:bench]},
      {:elixir_talk, "~> 1.1", only: [:bench]},
    ]
  end

end
