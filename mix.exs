defmodule Beanstix.MixProject do
  use Mix.Project

  def project do
    [
      app: :beanstix,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      description: description(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:connection, "~> 1.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      lint: ["format --check-formatted", "credo --strict"]
    ]
  end

  defp description() do
    "A beanstalkd client for Elixir."
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/fitronics/beanstix"}
    ]
  end
end
