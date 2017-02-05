defmodule Beanstix do

  @type command :: {atom}
  @default_timeout 5000

  @spec connect(binary | Keyword.t, Keyword.t) :: GenServer.on_start
  def connect(beanstalkd_opts \\ [], connection_opts \\ []) do
    Beanstix.Connection.start_link(beanstalkd_opts, connection_opts)
  end

  @doc """
  Closes the connection to the Beanstalkd server.

  This function is asynchronous: it returns `:ok` as soon as it's called and
  performs the closing of the connection after that.
  """
  @spec disconnect(GenServer.server) :: :ok
  def disconnect(pid) do
    Beanstix.Connection.stop(pid)
  end

  def command(pid, command, opts \\ []) do
    pipeline(pid, [command], opts)
  end

  def pipeline(pid, commands, opts \\ []) do
    Beanstix.Connection.pipeline(pid, commands, opts[:timeout] || @default_timeout)
  end

  @doc """
  This function formats an error reason into a human-readable string.

  This function can be used to turn an error reason (returned in
  `{:error, reason}` by `command/3` and `pipeline/3`) into a
  human-readable message string.
  """
  @spec format_error(term) :: binary
  def format_error(reason)

  # :inet.format_error/1 doesn't format :tcp_closed or :closed.
  def format_error(:tcp_closed) do
    "TCP connection closed"
  end

  # Manually returned when the connection is closed
  def format_error(:closed) do
    "the connection to Beanstalkd is closed"
  end

  def format_error(reason) do
    case :inet.format_error(reason) do
      'unknown POSIX error' -> inspect(reason)
      message -> List.to_string(message)
    end
  end

end
