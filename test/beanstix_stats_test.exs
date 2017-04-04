defmodule BeanstixStatsTest do
  use ExUnit.Case

  @moduletag :stats

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  @tag no_setup: true
  test "stats parse" do
    stats = File.read!("test/files/stats.txt")
    |> Beanstix.Stats.parse()

    assert stats["rusage-utime"] == 0.004
    assert stats["current-jobs-urgent"] == 4
    assert stats["id"] == "33ecd66987a51631"
  end

  test "stats-job", %{pool_name: pool_name, tube: tube} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, data} = Beanstix.command(pool_name, {:stats_job, job_id})
    assert data["tube"] == tube
    assert data["state"] == "ready"
    assert data["delay"] == 0
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
  end

  test "stats-tube", %{pool_name: pool_name, tube: tube} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, data} = Beanstix.command(pool_name, {:stats_tube, tube})
    assert data["name"] == tube
    assert data["current-jobs-ready"] == 1
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
    assert {:ok, data} = Beanstix.command(pool_name, {:stats_tube, tube})
    assert data["current-jobs-ready"] == 0
  end

  test "stats", %{pool_name: pool_name} do
    assert {:ok, data} = Beanstix.command(pool_name, :stats)
    assert %{"current-jobs-urgent" => _x} = data
  end

end
