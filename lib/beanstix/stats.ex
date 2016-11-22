defmodule Beanstix.Stats do

  @string_keys ["tube", "state", "id", "hostname"]
  @float_keys ["version", "rusage-utime", "rusage-stime"]

  def parse(stats) do
    parse(stats, byte_size(stats))
  end

  def parse(stats, length) do
    stats_length = length - 5
    # Remove begining and end
    <<"---\n", stats :: size(stats_length) - binary, "\n">> = stats

    # Split by line and parse into a map
    for line <- String.split(stats, "\n"), into: %{}, do: parse_line(line)
  end

  def parse_line(line) do
    line
    |> String.split(": ")
    |> format()
  end

  def format([k, v]) when k in @string_keys, do: {k, v}
  def format([k, v]) when k in @float_keys, do: {k, String.to_float(v)}
  def format([k, v]), do: {k, String.to_integer(v)}

end
