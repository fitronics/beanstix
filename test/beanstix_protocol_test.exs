defmodule BeanstixProtocolTest do
  use ExUnit.Case
  alias Beanstix.Protocol

  @moduletag :protocol

  test "parse_multi/2" do
    cmds = "USING qwerty\r\nTIMED_OUT\r\nINSERTED 12\r\n"
    reply = {[{:ok, "qwerty"}, {:ok, :timed_out}, {:ok, 12}], 0, ""}
    assert Protocol.parse_multi(cmds, 3) == reply

    cmds = "USING qwerty\r\nTIMED_OUT\r\nINSERT"
    reply = {[{:ok, "qwerty"}, {:ok, :timed_out}], 1, "INSERT"}
    assert Protocol.parse_multi(cmds, 3) == reply
  end

end
