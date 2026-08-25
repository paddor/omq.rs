defmodule Omq.MixProject do
  use Mix.Project

  def project do
    [
      app: :omq_elixir,
      version: "0.1.0",
      elixir: "~> 1.16",
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    []
  end
end
