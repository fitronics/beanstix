defmodule BeanstixStatsTest do
  use ExUnit.Case

  test "stats" do
    stats = File.read!("test/files/stats.txt")
    |> Beanstix.Stats.parse()

    assert stats["rusage-utime"] == 0.004
    assert stats["current-jobs-urgent"] == 4
    assert stats["id"] == "33ecd66987a51631"
  end

end
