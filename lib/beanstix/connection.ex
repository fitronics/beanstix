defmodule Beanstix.Connection do
  @moduledoc false

  use Connection

  alias Beanstix.Protocol

  require Logger

  defstruct [
    host: '127.0.0.1',
    port: 11300,
    opts: [],
    conn_timeout: 5_000,
    recv_timeout: 5_000,
    conn: nil
  ]

  @conn_opts [mode: :binary, packet: 0, active: false, reuseaddr: true]

  @spec start_link(Keyword.t, Keyword.t) :: GenServer.on_start
  def start_link(args, opts) do
    Connection.start_link(__MODULE__, args, opts)
  end

  @spec stop(GenServer.server) :: :ok
  def stop(pid) do
    Connection.cast(pid, :stop)
  end

  def command(pid, command, _timeout) do
    Connection.call(pid, {:command, command})
  end

  def pipeline(conn, commands, timeout) do
    request_id = make_ref()

    # All this try-catch dance is required in order to cleanly return {:error, :timeout}
    # on timeouts instead of exiting (which is what `GenServer.call/3` does).
    try do
      {^request_id, resp} = Connection.call(conn, {:commands, commands, request_id}, timeout)
      resp
    catch
      :exit, {:timeout, {:gen_server, :call, [^conn | _]}} ->
        Connection.call(conn, {:timed_out, request_id})

        # We try to flush the response because it may have arrived before the
        # connection processed the :timed_out message. In case it arrived, we
        # notify the connection that it arrived (canceling the :timed_out
        # message).
        receive do
          {ref, {^request_id, _resp}} when is_reference(ref) ->
            Connection.call(conn, {:cancel_timed_out, request_id})
        after
          0 -> :ok
        end

        {:error, :timeout}
    end
  end

  def init(args) do
    state = %__MODULE__{}
    |> Map.merge(Enum.into(args, %{}))
    {:connect, :init, state}
  end

  def connect(_info, %{conn: nil, host: host, port: port, opts: opts,
    conn_timeout: timeout} = state) do
    Logger.info("Connect with #{host}: #{port}")
    case :gen_tcp.connect(host, port, @conn_opts ++ opts, timeout) do
      {:ok, conn} ->
        {:ok, %{state | conn: conn}}
      {:error, _} ->
        {:backoff, 1000, state}
    end
  end

  @doc false
  def disconnect(reason, state)

  # We disconnect with reason :stop when we call Beanstix.stop/1.
  def disconnect(:stop, state) do
    {:stop, :normal, state}
  end

  def disconnect(info, %{conn: conn} = state) do
    :ok = :gen_tcp.close(conn)
    case info do
      {:close, from} ->
        Connection.reply(from, :ok)
      {:error, :closed} ->
        :error_logger.format("Connection closed~n", [])
      {:error, reason} ->
        reason = :inet.format_error(reason)
        :error_logger.format("Connection error: ~s~n", [reason])
    end
    {:connect, :reconnect, %{state | conn: nil}}
  end

  def handle_call(_, _, %{conn: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:recv, bytes, timeout}, _, %{conn: conn} = s) do
    case :gen_tcp.recv(conn, bytes, timeout) do
      {:ok, _} = ok ->
        {:reply, ok, s}
      {:error, :timeout} = timeout ->
        {:reply, timeout, s}
      {:error, _} = error ->
        {:disconnect, error, error, s}
    end
  end
  def handle_call(:close, from, s) do
    {:disconnect, {:close, from}, s}
  end

  def handle_call({:command, {:put, data}}, from, state) do
    handle_call({:command, {:put, data, []}}, from, state)
  end
  def handle_call({:command, {:put, data, opts}}, _from, state) do
    pri   = Keyword.get(opts, :pri, 0)
    delay = Keyword.get(opts, :delay, 0)
    ttr   = Keyword.get(opts, :ttr, 60)
    bytes = byte_size(data)
    msg = "put #{pri} #{delay} #{ttr} #{bytes}\r\n#{data}\r\n"
    send_msg(msg, state)
  end
  def handle_call({:command, {cmd, data}}, _from, state) do
    cmd = atom_to_cmd(cmd)
    msg = cond do
      data == [] -> "#{cmd}\r\n"
      true       -> "#{cmd} #{data}\r\n"
    end
    send_msg(msg, state)
  end
  def handle_call({:command, cmd}, _from, state) do
    msg = atom_to_cmd(cmd) <> "\r\n"
    send_msg(msg, state)
  end

  defp send_msg(msg, %{conn: conn, recv_timeout: timeout} = state) do
    Logger.info("send_msg: #{inspect msg}")
    case :gen_tcp.send(conn, msg) do
      :ok ->
        case recv_msg(conn, <<>>, timeout) do
          {:ok, job_id, result} -> {:reply, {:ok, job_id, result}, state}
          {:ok, result} -> {:reply, {:ok, result}, state}
          {:error, err} -> {:reply, {:error, err}, state}
        end
      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp recv_msg(conn, buffer, timeout) do
    case :gen_tcp.recv(conn, 0, timeout) do
      {:ok, data} ->
        Logger.info("recv_msg: #{inspect data}")
        packet = <<buffer :: binary, data :: binary>>
        case Protocol.parse(packet) do
          :more -> recv_msg(conn, packet, timeout)
          {:ok, result, _rest} -> {:ok, result}
          {:ok, job_id, result, _rest} -> {:ok, job_id, result}
          {:error, result, _rest} -> {:error, result}
        end
      error ->
        error
    end
  end

  defp atom_to_cmd(cmd) when is_atom(cmd) do
    cmd
    |> Atom.to_string()
    |> String.replace("_", "-")
  end

end
