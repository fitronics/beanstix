defmodule Beanstix.Stats do
  @moduledoc """
  Stats parsing for Beanstix
  """

  @string_keys ~w(tube state name hostname)
  @float_keys ~w(version rusage-utime rusage-stime)
  @integer_keys ~w(pri age delay ttr time-left file reserves timeouts releases buries kicks
    current-jobs-urgent current-jobs-ready current-jobs-reserved current-jobs-delayed
    current-jobs-buried cmd-put cmd-peek cmd-peek-ready cmd-peek-delayed cmd-peek-buried cmd-reserve
    cmd-reserve-with-timeout cmd-delete cmd-release cmd-use cmd-watch cmd-ignore cmd-bury cmd-kick cmd-touch
    cmd-stats cmd-stats-job cmd-stats-tube cmd-list-tubes cmd-list-tube-used cmd-list-tubes-watched
    cmd-pause-tube job-timeouts total-jobs max-job-size current-tubes current-connectionscurrent-producers
    current-workers current-waiting total-connections pid uptime binlog-oldest-index binlog-current-index
    binlog-records-migrated binlog-records-written binlog-max-size)

  def parse(stats) do
    stats =
      Enum.reduce(String.split(stats, "\n"), {:list, []}, fn x, {type, acc} ->
        case parse_line(x) do
          nil -> {type, acc}
          {k, v} -> {:map, [{k, v} | acc]}
          x -> {:list, [x | acc]}
        end
      end)

    case stats do
      {:list, x} -> x
      {:map, x} -> Enum.into(x, %{})
    end
  end

  # id can be integer or string
  def parse_line(<<"id: ", rest::binary>>) do
    try do
      {"id", String.to_integer(rest)}
    rescue
      ArgumentError -> {"id", rest}
    end
  end

  for key <- @integer_keys do
    def parse_line(<<unquote(key), ": ", rest::binary>>) do
      {unquote(key), String.to_integer(rest)}
    end
  end

  for key <- @float_keys do
    def parse_line(<<unquote(key), ": ", rest::binary>>) do
      {unquote(key), String.to_float(rest)}
    end
  end

  for key <- @string_keys do
    def parse_line(<<unquote(key), ": ", rest::binary>>) do
      {unquote(key), rest}
    end
  end

  def parse_line(<<"- ", rest::binary>>) do
    rest
  end

  def parse_line(_) do
    nil
  end
end
