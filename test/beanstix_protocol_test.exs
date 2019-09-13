defmodule BeanstixProtocolTest do
  use ExUnit.Case
  alias Beanstix.Protocol

  @moduletag :protocol

  test "build_commands/1" do
    cmds = [{:use, "test"}, {:put, "test"}]
    cmd_string = "use test\r\nput 0 0 180 4\r\ntest\r\n"
    result_string = Protocol.build_commands(cmds) |> to_string()
    assert result_string == cmd_string

    cmds = [{:put, <<1, 2, 3>>}]
    cmd_string = <<112, 117, 116, 32, 48, 32, 48, 32, 49, 56, 48, 32, 51, 13, 10, 1, 2, 3, 13, 10>>
    result_string = Protocol.build_commands(cmds) |> to_string()
    assert result_string == cmd_string
  end

  test "parse_multi/2" do
    cmds = "USING qwerty\r\nTIMED_OUT\r\nINSERTED 12\r\n"
    reply = {"", 0, [ok: 12, ok: :timed_out, ok: "qwerty"]}
    assert Protocol.parse_multi(cmds, 3) == reply

    cmds = "USING qwerty\r\nTIMED_OUT\r\nINSERT"
    reply = {"INSERT", 1, [ok: :timed_out, ok: "qwerty"]}
    assert Protocol.parse_multi(cmds, 3) == reply
  end
end
