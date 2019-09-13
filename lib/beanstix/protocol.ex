defmodule Beanstix.Protocol do
  @moduledoc """
  Protocol parsing for Beanstix
  """
  require Logger

  alias Beanstix.{Error, ParseError, Stats}

  @crlf "\r\n"
  @default_priority 0

  @type beanstalkd_value :: binary | integer | [beanstalkd_value]

  def format_command({:put, data}), do: format_command({:put, data, []})

  def format_command({:put, data, opts}) when is_binary(data) do
    priority = Keyword.get(opts, :priority, @default_priority)
    delay = Keyword.get(opts, :delay, 0)
    timeout = Keyword.get(opts, :timeout, 180)
    bytes = byte_size(data)
    ["put ", to_string(priority), " ", to_string(delay), " ", to_string(timeout), " ", to_string(bytes), @crlf, data]
  end

  def format_command({:put, data, opts}), do: format_command({:put, to_string(data), opts})

  def format_command({:reserve, opts}) do
    case Keyword.get(opts, :timeout, :infinity) do
      :infinity ->
        ["reserve"]

      timeout ->
        ["reserve-with-timeout #{timeout}"]
    end
  end

  def format_command({:release, job_id}), do: format_command({:release, job_id, []})

  def format_command({:release, job_id, opts}) do
    priority = Keyword.get(opts, :priority, @default_priority)
    delay = Keyword.get(opts, :delay, 0)
    ["release ", "#{job_id} #{priority} #{delay}"]
  end

  def format_command({:bury, job_id}), do: format_command({:bury, job_id, []})

  def format_command({:bury, job_id, opts}) do
    priority = Keyword.get(opts, :priority, @default_priority)
    ["bury ", "#{job_id} #{priority}"]
  end

  def format_command(:kick), do: format_command({:kick, []})
  def format_command({:kick}), do: format_command({:kick, []})

  def format_command({:kick, opts}) do
    bound = Keyword.get(opts, :bound, 1)
    ["kick ", "#{bound}"]
  end

  def format_command({:pause_tube, tube}), do: format_command({:pause_tube, tube, []})

  def format_command({:pause_tube, tube, opts}) do
    delay = Keyword.get(opts, :delay, 0)
    ["pause-tube ", "#{tube} #{delay}"]
  end

  def format_command({cmd, data}) when is_atom(cmd) do
    [atom_to_cmd(cmd), " #{data}"]
  end

  def format_command({cmd}) when is_atom(cmd) do
    atom_to_cmd(cmd)
  end

  def format_command(cmd) when is_atom(cmd) do
    atom_to_cmd(cmd)
  end

  def format_command(cmd) do
    raise %Error{message: "Unknown Invalid command (#{inspect(cmd)})"}
  end

  def build_commands(commands) do
    commands
    |> Enum.map(&build_command/1)
  end

  def build_command(command) do
    [format_command(command), @crlf]
  end

  @spec parse_multi(binary, non_neg_integer) :: {:ok, [beanstalkd_value], binary} | {:error, term}
  def parse_multi(data, n) do
    parse_multi(data, n, [])
  end

  def parse_multi(data, 0, acc), do: {acc, 0, data}

  def parse_multi(data, n, acc) do
    case parse(data) do
      :incomplete ->
        {data, n, acc}

      {status, resp, ""} ->
        {"", n - 1, [{status, resp} | acc]}

      {status, resp, rest} ->
        parse_multi(rest, n - 1, [{status, resp} | acc])
    end
  end

  @spec parse(binary) :: {:ok, term, binary} | :more
  def parse(<<"OUT_OF_MEMORY\r\n", rest::binary>>), do: error("OUT_OF_MEMORY", rest)
  def parse(<<"INTERNAL_ERROR\r\n", rest::binary>>), do: error("INTERNAL_ERROR", rest)
  def parse(<<"DRAINING\r\n", rest::binary>>), do: error("DRAINING", rest)
  def parse(<<"BAD_FORMAT\r\n", rest::binary>>), do: error("BAD_FORMAT", rest)
  def parse(<<"UNKNOWN_COMMAND\r\n", rest::binary>>), do: error("UNKNOWN_COMMAND", rest)
  def parse(<<"EXPECTED_CRLF\r\n", rest::binary>>), do: error("EXPECTED_CRLF", rest)
  def parse(<<"JOB_TOO_BIG\r\n", rest::binary>>), do: error("JOB_TOO_BIG", rest)
  def parse(<<"DEADLINE_SOON\r\n", rest::binary>>), do: {:ok, :deadline_soon, rest}
  def parse(<<"TIMED_OUT\r\n", rest::binary>>), do: {:ok, :timed_out, rest}
  def parse(<<"DELETED\r\n", rest::binary>>), do: {:ok, :deleted, rest}
  def parse(<<"NOT_FOUND\r\n", rest::binary>>), do: {:ok, :not_found, rest}
  def parse(<<"RELEASED\r\n", rest::binary>>), do: {:ok, :released, rest}
  def parse(<<"BURIED\r\n", rest::binary>>), do: {:ok, :buried, rest}
  def parse(<<"TOUCHED\r\n", rest::binary>>), do: {:ok, :touched, rest}
  def parse(<<"NOT_IGNORED\r\n", rest::binary>>), do: {:ok, :not_ignored, rest}
  def parse(<<"KICKED\r\n", rest::binary>>), do: {:ok, :kicked, rest}
  def parse(<<"PAUSED\r\n", rest::binary>>), do: {:ok, :paused, rest}
  def parse(<<"INSERTED ", rest::binary>>), do: parse_integer(rest)
  def parse(<<"BURIED ", rest::binary>>), do: parse_integer(:buried, rest)
  def parse(<<"WATCHING ", rest::binary>>), do: parse_integer(rest)
  def parse(<<"KICKED ", rest::binary>>), do: parse_integer(rest)
  def parse(<<"USING ", rest::binary>>), do: parse_string(rest)
  def parse(<<"RESERVED ", rest::binary>>), do: parse_job(rest)
  def parse(<<"FOUND ", rest::binary>>), do: parse_job(rest)
  def parse(<<"OK ", rest::binary>>), do: parse_stats(rest)
  def parse(_), do: :incomplete

  defp error(message, rest) do
    {:error, %Error{message: message}, rest}
  end

  defp parse_result(result, status) do
    case result do
      :incomplete -> :incomplete
      {:ok, acc, rest} -> {:ok, {status, acc}, rest}
    end
  end

  # Parsers
  defp parse_integer(status, bin) do
    parse_integer(bin)
    |> parse_result(status)
  end

  defp parse_integer(<<digit, _::binary>> = bin) when digit in ?0..?9 do
    parse_digits(bin, 0)
  end

  defp parse_integer(<<non_digit, _::binary>>) do
    raise ParseError, message: "expected integer, found: #{inspect(<<non_digit>>)}"
  end

  defp parse_digits(<<digit, rest::binary>>, acc) when digit in ?0..?9,
    do: parse_digits(rest, acc * 10 + (digit - ?0))

  defp parse_digits(<<@crlf, rest::binary>>, acc),
    do: {:ok, acc, rest}

  defp parse_digits(<<_non_digit, _::binary>> = rest, acc),
    do: {:ok, acc, rest}

  defp parse_digits(<<>>, _),
    do: :incomplete

  defp parse_string(bin) do
    until_crlf(bin)
  end

  defp parse_job(bin) do
    case parse_id(bin) do
      {:ok, id, bin} ->
        case parse_body(bin) do
          {:ok, body, _len, rest} ->
            {:ok, {id, body}, rest}

          _ ->
            :incomplete
        end

      _ ->
        :incomplete
    end
  end

  defp parse_id(bin) do
    case parse_digits(bin, 0) do
      {:ok, id, <<" ", rest::binary>>} ->
        {:ok, id, rest}

      _ ->
        :incomplete
    end
  end

  defp parse_body(bin) do
    case parse_integer(bin) do
      {:ok, len, rest} ->
        parse_string_of_known_size(rest, len)

      _ ->
        :incomplete
    end
  end

  defp parse_string_of_known_size(data, len) do
    case data do
      <<str::bytes-size(len), @crlf, rest::binary>> ->
        {:ok, str, len, rest}

      _ ->
        :incomplete
    end
  end

  defp parse_stats(bin) do
    case parse_body(bin) do
      {:ok, body, _, rest} ->
        {:ok, Stats.parse(body), rest}

      _ ->
        :incomplete
    end
  end

  defp until_crlf(data, acc \\ "")
  defp until_crlf("", _), do: :incomplete
  defp until_crlf(@crlf <> rest, acc), do: {:ok, acc, rest}
  defp until_crlf(<<h, rest::binary>>, acc), do: until_crlf(rest, <<acc::binary, h>>)

  defp atom_to_cmd(cmd) when is_atom(cmd) do
    cmd
    |> Atom.to_string()
    |> String.replace("_", "-")
  end
end
