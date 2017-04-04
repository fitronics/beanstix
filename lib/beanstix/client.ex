defmodule Beanstix.Client do
  @moduledoc """
  Beanstix client process
  """
  @behaviour :shackle_client

  alias Beanstix.Protocol

  @max_32_bit_int 4_294_967_296

  @type state :: %__MODULE__{}

  defstruct [
    queue: [],
    request_id: 0,
    buffer: "",
  ]

  def init do
    state = %__MODULE__{}
    {:ok, state}
  end

  def setup(_socket, state) do
    {:ok, state}
  end

  def handle_request(request, state) do
    # IO.inspect(request, label: "handle_request_1")
    request_id = request_id(state.request_id)
    num_cmds = length(request)
    commands = request
    |> Protocol.build_commands()
    queue = state.queue ++ [{request_id, num_cmds}]
    # IO.inspect(commands, label: "handle_request_2")
    {:ok, request_id, commands, %{state | request_id: request_id, queue: queue}}
  end

  def handle_data(data, state) do
    # IO.inspect({state, data}, label: "handle_data_1")
    {rest, queue, reply} = parse(state.buffer <> data, state.queue, [])
    # IO.inspect({rest, reply, queue}, label: "handle_data_2")
    {:ok, reply, %{state | buffer: rest, queue: queue}}
  end

  def terminate(_state) do
    :ok
  end

  defp parse("", queue, reply), do: {"", queue, reply}
  defp parse(data, [], reply), do: {data, [], reply}
  defp parse(data, [{request_id, num_cmds} | tail] = queue, reply) do
    case Protocol.parse_multi(data, num_cmds) do
      {:incomplete, data} ->
        {data, queue, reply}
      {status, resp, rest} when is_atom(status) ->
        # IO.inspect({status, resp, rest}, label: "parse1")
        parse(rest, tail, reply ++ [{request_id , {status, resp}}])
      {resp, 0, rest} when is_list(resp) ->
        # IO.inspect(resp, label: "parse2")
        parse(rest, tail, reply ++ [{request_id , resp}])
      _ ->
        # IO.inspect(x, label: "parse3")
        {data, queue, reply}
    end
  end

  defp request_id(num) do
    rem((num + 1), @max_32_bit_int)
  end
end
