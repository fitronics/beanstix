defmodule Beanstix.Protocol do
  require Logger

  alias Beanstix.{Error, ParseError, Stats}

  @crlf "\r\n"
  @default_priority :math.pow(2, 31) |> round

  @type beanstalkd_value :: binary | integer | [beanstalkd_value]

  def format_command({:put, data}), do: format_command({:put, data, []})
  def format_command({:put, data, opts}) when is_binary(data) do
    priority = Keyword.get(opts, :priority, @default_priority)
    delay = Keyword.get(opts, :delay, 0)
    timeout = Keyword.get(opts, :timeout, 180)
    bytes = byte_size(data)
    ["put ", "#{priority} #{delay} #{timeout} #{bytes}", @crlf, data]
  end
  def format_command({:put, data, opts}), do: format_command({:put, to_string(data), opts})

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
    %Error{message: "Unknown Invalid command (#{inspect cmd})"}
  end

  @spec pack(binary) :: iodata
  def pack(command) do
    [format_command(command), @crlf]
    |> to_string()
  end

  @spec parse(binary) :: {:ok, term, binary} | :more
  def parse(<<"OUT_OF_MEMORY\r\n", _ :: binary>> = data),   do: parse_error(data)
  def parse(<<"INTERNAL_ERROR\r\n", _ :: binary>> = data),  do: parse_error(data)
  def parse(<<"DRAINING\r\n", _ :: binary>> = data),        do: parse_error(data)
  def parse(<<"BAD_FORMAT\r\n", _ :: binary>> = data),      do: parse_error(data)
  def parse(<<"UNKNOWN_COMMAND\r\n", _ :: binary>> = data), do: parse_error(data)
  def parse(<<"EXPECTED_CRLF\r\n", _ :: binary>> = data),   do: parse_error(data)
  def parse(<<"JOB_TOO_BIG\r\n", _ :: binary>> = data),     do: parse_error(data)
  def parse(<<"DEADLINE_SOON\r\n", rest :: binary>>),       do: {:ok, :deadline_soon, rest}
  def parse(<<"TIMED_OUT\r\n", rest :: binary>>),           do: {:ok, :timed_out, rest}
  def parse(<<"DELETED\r\n", rest :: binary>>),             do: {:ok, :deleted, rest}
  def parse(<<"NOT_FOUND\r\n", rest :: binary>>),           do: {:ok, :not_found, rest}
  def parse(<<"RELEASED\r\n", rest :: binary>>),            do: {:ok, :released, rest}
  def parse(<<"BURIED\r\n", rest :: binary>>),              do: {:ok, :buried, rest}
  def parse(<<"TOUCHED\r\n", rest :: binary>>),             do: {:ok, :touched, rest}
  def parse(<<"NOT_IGNORED\r\n", rest :: binary>>),         do: {:ok, :not_ignored, rest}
  def parse(<<"PAUSED\r\n", rest :: binary>>),              do: {:ok, :paused, rest}
  def parse(<<"INSERTED ", rest :: binary>>),               do: parse_integer(rest)
  def parse(<<"BURIED ", rest :: binary>>),                 do: parse_integer(rest)
  def parse(<<"WATCHING ", rest :: binary>>),               do: parse_integer(rest)
  def parse(<<"KICKED ", rest :: binary>>),                 do: parse_integer(rest)
  def parse(<<"USING ", rest :: binary>>),                  do: parse_string(rest)
  def parse(<<"RESERVED ", rest :: binary>>),               do: parse_job(rest)
  def parse(<<"FOUND ", rest :: binary>>),                  do: parse_job(rest)
  def parse(<<"OK ", rest :: binary>>),                     do: parse_stats(rest)
  def parse(data) do
    raise(ParseError, message: "Invalid command (#{inspect data})")
  end

  @spec parse_multi(binary, non_neg_integer) :: {:ok, [beanstalkd_value], binary} | {:error, term}
  def parse_multi(data, nelems)

  # We treat the case when we have just one element to parse differently as it's
  # a very common case since single commands are treated as pipelines with just
  # one command in them.
  def parse_multi(data, 1) do
    resolve_cont(parse(data), &{:ok, &1, &2})
  end

  def parse_multi(data, n) do
    take_elems(data, n, [])
  end

  # Type parsers

  defp parse_error(data) do
    data
    |> until_crlf()
    |> resolve_cont(&{:ok, %Error{message: &1}, &2})
  end

  defp parse_integer("") do
    mkcont(&parse_integer/1)
  end

  defp parse_integer(<<digit, _ :: binary>> = bin) when digit in ?0..?9 do
    resolve_cont parse_digits(bin, 0), fn(i, rest) ->
      resolve_cont until_crlf(rest), fn
        "", rest ->
          {:ok, i, rest}
        <<char, _ :: binary>>, _rest ->
          raise ParseError, message: "expected CRLF, found: #{inspect <<char>>}"
      end
    end
  end

  defp parse_integer(<<non_digit, _ :: binary>>) do
    raise ParseError, message: "expected integer, found: #{inspect <<non_digit>>}"
  end

  defp parse_digits(<<digit, rest :: binary>>, acc) when digit in ?0..?9,
    do: parse_digits(rest, acc * 10 + (digit - ?0))
  defp parse_digits(<<_non_digit, _ :: binary>> = rest, acc),
    do: {:ok, acc, rest}
  defp parse_digits(<<>>, acc),
    do: mkcont(&parse_digits(&1, acc))

  defp parse_string(bin) do
    until_crlf(bin)
  end

  defp parse_job(bin) do
    case parse_id(bin) do
      {:ok, id, bin} ->
        case parse_body(bin) do
          {:ok, body, _len, rest} ->
            {:ok, {id, body}, rest}
          fun ->
            fun
        end
      fun ->
        fun
    end
  end

  defp parse_id(bin) do
    case parse_digits(bin, 0) do
      {:ok, id, <<" ", rest :: binary>>} ->
        {:ok, id, rest}
       fun ->
        fun
    end
  end

  defp parse_body(bin) do
    case parse_integer(bin) do
      {:ok, len, rest} ->
        parse_string_of_known_size(rest, len)
      fun ->
        fun
    end
  end

  defp parse_string_of_known_size(data, len) do
    case data do
      <<str :: bytes-size(len), @crlf, rest :: binary>> ->
        {:ok, str, len, rest}
      _ ->
        mkcont fn(new_data) -> parse_string_of_known_size(data <> new_data, len) end
    end
  end

  defp parse_stats(bin) do
    case parse_body(bin) do
      {:ok, body, len, rest} ->
        {:ok, Stats.parse(body, len), rest}
      fun ->
        fun
    end
  end

  defp until_crlf(data, acc \\ "")

  defp until_crlf(@crlf <> rest, acc),         do: {:ok, acc, rest}
  defp until_crlf("", acc),                    do: mkcont(&until_crlf(&1, acc))
  defp until_crlf("\r", acc),                  do: mkcont(&until_crlf(<<?\r, &1 :: binary>>, acc))
  defp until_crlf(<<h, rest :: binary>>, acc), do: until_crlf(rest, <<acc :: binary, h>>)

  defp take_elems(data, 0, acc) do
    {:ok, Enum.reverse(acc), data}
  end

  defp take_elems(<<_, _ :: binary>> = data, n, acc) when n > 0 do
    resolve_cont parse(data), fn(elem, rest) ->
      take_elems(rest, n - 1, [elem | acc])
    end
  end

  defp take_elems(<<>>, n, acc) do
    mkcont(&take_elems(&1, n, acc))
  end

  defp resolve_cont({:ok, val, rest}, ok) when is_function(ok, 2),
    do: ok.(val, rest)
  defp resolve_cont({:error, val, _rest}, ok) when is_function(ok, 2),
    do: {:error, val}
  defp resolve_cont({:continuation, cont}, ok),
    do: mkcont(fn(new_data) -> resolve_cont(cont.(new_data), ok) end)

  @compile {:inline, mkcont: 1}
  defp mkcont(fun) do
    {:continuation, fun}
  end

  defp atom_to_cmd(cmd) when is_atom(cmd) do
    cmd
    |> Atom.to_string()
    |> String.replace("_", "-")
  end

end
