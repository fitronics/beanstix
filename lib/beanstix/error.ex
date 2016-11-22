defmodule Beanstix.Error do
  @moduledoc """
  Error returned by Beanstalkd.
  """

  defexception [:message]

  @type t :: %__MODULE__{message: binary}
end

defmodule Beanstix.ConnectionError do
  @moduledoc """
  Error in the connection to Beanstalkd.
  """

  defexception [:message]

  def exception(reason) when is_binary(reason),
    do: %__MODULE__{message: reason}
  def exception(reason),
    do: %__MODULE__{message: Beanstix.format_error(reason)}
end
