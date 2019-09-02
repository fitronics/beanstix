defmodule BeanstixProtocolTest do
  use ExUnit.Case
  alias Beanstix.Protocol

  @moduletag :protocol

  test "parse_multi/2" do
    cmds = "USING qwerty\r\nTIMED_OUT\r\nINSERTED 12\r\n"
    reply = {"", 0, [ok: 12, ok: :timed_out, ok: "qwerty"]}
    assert Protocol.parse_multi(cmds, 3) == reply

    cmds = "USING qwerty\r\nTIMED_OUT\r\nINSERT"
    reply = {"INSERT", 1, [ok: :timed_out, ok: "qwerty"]}
    assert Protocol.parse_multi(cmds, 3) == reply
  end
end
