defmodule BeanstixStatsTest do
  use ExUnit.Case

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  test "stats parse" do
    stats = File.read!("test/files/stats.txt")
    |> Beanstix.Stats.parse()

    assert stats["rusage-utime"] == 0.004
    assert stats["current-jobs-urgent"] == 4
    assert stats["id"] == "33ecd66987a51631"
  end

  test "stats", %{pid: pid} do
    assert {:ok, data} = Beanstix.command(pid, :stats)
    assert %{"current-jobs-urgent" => _x} = data
  end



end
