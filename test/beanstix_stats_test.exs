defmodule BeanstixStatsTest do
  use ExUnit.Case

  @moduletag :stats

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  @tag no_setup: true
  test "stats parse" do
    stats =
      File.read!("test/files/stats.txt")
      |> Beanstix.Stats.parse()

    assert stats["rusage-utime"] == 0.004
    assert stats["current-jobs-urgent"] == 4
    assert stats["id"] == "33ecd66987a51631"
  end

  @tag no_setup: true
  test "stats-tube parse" do
    stats =
      File.read!("test/files/stats-tube.txt")
      |> Beanstix.Stats.parse()

    assert stats["name"] == "default"
    assert stats["current-jobs-buried"] == 3
    assert stats["current-jobs-urgent"] == 0
  end

  @tag no_setup: true
  test "stats-job parse" do
    stats =
      File.read!("test/files/stats-job.txt")
      |> Beanstix.Stats.parse()

    assert stats["id"] == 2
    assert stats["tube"] == "default"
    assert stats["reserves"] == 2
    assert stats["age"] == 15872424
  end

  @tag no_setup: true
  test "list-tubes parse" do
    tubes =
      File.read!("test/files/list-tubes.txt")
      |> Beanstix.Stats.parse()

    assert length(tubes) == 4
    assert "default" in tubes
    assert "tube1" in tubes
    assert "tube2" in tubes
    assert "tube3" not in tubes
    assert "another-tube" in tubes
  end

  test "stats-job", %{pid: pid, tube: tube} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, data} = Beanstix.command(pid, {:stats_job, job_id})
    assert data["tube"] == tube
    assert data["state"] == "ready"
    assert data["delay"] == 0
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "stats-tube", %{pid: pid, tube: tube} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, data} = Beanstix.command(pid, {:stats_tube, tube})
    assert data["name"] == tube
    assert data["current-jobs-ready"] == 1
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
    assert {:ok, data} = Beanstix.command(pid, {:stats_tube, tube})
    assert data["current-jobs-ready"] == 0
  end

  test "stats", %{pid: pid} do
    assert {:ok, data} = Beanstix.command(pid, :stats)
    assert %{"current-jobs-urgent" => _x} = data
  end
end
