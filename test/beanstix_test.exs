defmodule BeanstixTest do
  use ExUnit.Case
  doctest Beanstix

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  test "command/2", %{pid: pid} do
    assert {:error, %Beanstix.Error{message: "UNKNOWN_COMMAND"}} = Beanstix.command(pid, :peak_ready)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
  end

  test "put", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "put priority", %{pid: pid} do
    data1 = "1"
    data2 = "2"
    assert {:ok, job_id1} = Beanstix.command(pid, {:put, data1, priority: 9999})
    assert {:ok, job_id2} = Beanstix.command(pid, {:put, data2, priority: 999})
    assert {:ok, {^job_id2, ^data2}} = Beanstix.command(pid, :reserve)
    assert {:ok, {^job_id1, ^data1}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id1})
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id2})
  end

  # test "put timeout", %{pid: pid} do
  #   data = "1"
  #   assert {:ok, job_id} = Beanstix.command(pid, {:put, data, timeout: 1})
  #   assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
  #   :timer.sleep(:timer.seconds(1))
  #   assert {:ok, :not_found} = Beanstix.command(pid, {:delete, job_id})
  #   assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
  #   assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  # end

  test "use", %{pid: pid} do
    tube = "Beanstix_test_tube"
    assert {:ok, ^tube} = Beanstix.command(pid, {:use, tube})
  end

  test "reserve-with-timeout", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, {:reserve_with_timeout, 0})
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "release", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
    assert {:ok, :released} = Beanstix.command(pid, {:release, job_id})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "bury", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :buried} = Beanstix.command(pid, {:bury, job_id})
    assert {:ok, :not_found} = Beanstix.command(pid, {:bury, job_id})
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
    assert {:ok, 1} = Beanstix.command(pid, :kick)
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "list-tubes", %{pid: pid} do
    assert {:ok, tubes} = Beanstix.command(pid, :list_tubes)
    assert "default" in tubes
  end

  test "list-tubes-watched", %{pid: pid, tube: tube} do
    assert {:ok, tubes} = Beanstix.command(pid, :list_tubes_watched)
    assert tube in tubes
  end

  test "list-tube-used", %{pid: pid, tube: tube} do
    assert {:ok, ^tube} = Beanstix.command(pid, :list_tube_used)
  end

  test "pause-tube", %{pid: pid, tube: tube} do
    assert {:ok, :paused} = Beanstix.command(pid, {:pause_tube, tube})
    assert {:ok, :paused} = Beanstix.command(pid, {:pause_tube, tube, delay: 1})
  end

end
