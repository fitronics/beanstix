defmodule Beanstix.Connection do
  @moduledoc """
  Connection to beanstalkd
  """
  use Connection
  require Logger

  alias Beanstix.Protocol

  defmodule State do
    @moduledoc false
    defstruct host: '127.0.0.1',
              port: 11300,
              conn: nil,
              from: nil,
              connect_timeout: 5_000,
              recv_timeout: 5_000,
              reconnect: true,
              backoff: nil,
              backoff_max: 30_000
  end

  @backoff_exponent 1.5
  @sock_opts [mode: :binary, packet: 0, active: false, reuseaddr: true]

  def start_link(opts) do
    Connection.start_link(__MODULE__, opts, [])
  end

  def quit(pid) do
    Connection.cast(pid, :stop)
  end

  def call(pid, oper_with_data, timeout \\ 5_000) do
    Connection.call(pid, oper_with_data, timeout)
  end

  def init(opts) do
    Process.flag(:trap_exit, true)

    host = Keyword.get(opts, :host)
    port = Keyword.get(opts, :port)

    recv_timeout = Keyword.get(opts, :recv_timeout, 5_000)
    connect_timeout = Keyword.get(opts, :connect_timeout, 5_000)

    reconnect = Keyword.get(opts, :reconnect, true)

    state = %State{
      host: host,
      port: port,
      recv_timeout: recv_timeout,
      connect_timeout: connect_timeout,
      reconnect: reconnect
    }

    {:connect, :init, state}
  end

  def connect(
        _,
        %State{
          conn: nil,
          host: host,
          port: port,
          connect_timeout: timeout,
          reconnect: reconnect,
          backoff: backoff,
          backoff_max: backoff_max
        } = state
      ) do
    case :gen_tcp.connect(host, port, @sock_opts, timeout) do
      {:ok, conn} ->
        {:ok, %{state | conn: conn}}

      {:error, error} ->
        if reconnect do
          # Retry connection with backoff
          {:backoff, calc_next_backoff(backoff, backoff_max), state}
        else
          {:stop, error, state}
        end
    end
  end

  def disconnect(info, %State{conn: conn, reconnect: reconnect} = state) do
    :ok = :gen_tcp.close(conn)

    case info do
      {:close, from} ->
        Connection.reply(from, :ok)

      {:error, _} ->
        # Socket was likely closed on the other end
        :noop
    end

    if reconnect do
      {:connect, :reconnect, %{state | conn: nil}}
    else
      {:noconnect, %{state | conn: nil}}
    end
  end

  def handle_call(_, _, %State{conn: nil} = state) do
    {:reply, :closed, state}
  end

  def handle_call(cmds, _from, state) do
    num_cmds = length(cmds)

    Protocol.build_commands(cmds)
    |> send_msg(num_cmds, state)
  end

  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  def terminate(_, %State{conn: nil}) do
    :ok
  end

  def terminate(_, %State{conn: conn}) do
    :gen_tcp.close(conn)
    :ok
  end

  defp send_msg(cmds, num_cmds, %State{conn: conn, recv_timeout: timeout} = state) do
    case :gen_tcp.send(conn, cmds) do
      :ok ->
        case recv_msg(conn, num_cmds, [], <<>>, timeout) do
          {:ok, result} ->
            {:reply, result, state}

          {:error, :timeout} ->
            {:reply, :timeout, state}

          {:error, error} ->
            {:disconnect, {:error, error}, error, state}
        end

      {:error, error} ->
        {:disconnect, {:error, error}, error, state}
    end
  end

  defp recv_msg(conn, num_cmds, acc, buffer, timeout) do
    case :gen_tcp.recv(conn, 0, timeout) do
      {:ok, data} ->
        packet = <<buffer::binary, data::binary>>

        case Protocol.parse_multi(packet, num_cmds, acc) do
          {_, 0, acc} ->
            {:ok, Enum.reverse(acc)}

          {rest, num_cmds, acc} ->
            recv_msg(conn, num_cmds, acc, rest, timeout)
        end

      error ->
        error
    end
  end

  defp calc_next_backoff(backoff, backoff_max) do
    case {backoff, backoff_max} do
      {nil, _} -> 1_000
      {x, :infinity} -> round(x * @backoff_exponent)
      {x, backoff_max} -> min(round(x * @backoff_exponent), backoff_max)
    end
  end
end
