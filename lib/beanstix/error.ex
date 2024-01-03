defmodule Beanstix.Error do
  @moduledoc """
  Error returned by Beanstalkd.
  """

  defexception [:message]

  @type t :: %__MODULE__{message: binary}

  @doc """
  This function formats an error reason into a human-readable string.

  This function can be used to turn an error reason (returned in
  `{:error, reason}` by `command/3` and `pipeline/3`) into a
  human-readable message string.
  """
  @spec format_error(:inet.posix() | :tcp_closed | :closed) :: binary
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
      ~c"unknown POSIX error" -> inspect(reason)
      message -> List.to_string(message)
    end
  end
end

defmodule Beanstix.ParseError do
  @moduledoc """
  Error in parsing data according to the
  [RESP](https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt) protocol.
  """

  defexception [:message]
end
