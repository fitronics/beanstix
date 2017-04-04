defmodule Beanstix do
  @moduledoc """
  A beanstalkd client for elixir using shackle network client

  For more information on the beanstalkd protocol see
  [https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt](https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt)
  """

  @type result :: {:ok, non_neg_integer} |
                  {:ok, atom} |
                  {:error, :job_too_big} |
                  {:error, :draining}

  @pool_name :BeanstixPool

  def pool_name, do: @pool_name
  def opts_pool_name(opts), do: Keyword.pop(opts, :pool_name, @pool_name)

  def command(command), do: command(@pool_name, command)
  def command(pool_name, command) do
    :shackle.call(pool_name, [command])
  end

  def pipeline(commands), do: pipeline(@pool_name, commands)
  def pipeline(pool_name, commands) do
    :shackle.call(pool_name, commands)
  end

  @doc """
  Put a job in the current tube.
  The opts can be any combination of
    * `:pool_name` - if you have multiple pools you can specify the pool_name
    * `:pri` - an integer < 2**32. Jobs with smaller priority values will be
      scheduled before jobs with larger priorities. The most urgent priority is 0;
      the least urgent priority is 4,294,967,295.
    * `:delay` - an integer number of seconds to wait before putting the job in
      the ready queue. The job will be in the "delayed" state during this time.
    * `:ttr` -time to run -- is an integer number of seconds to allow a worker
      to run this job. This time is counted from the moment a worker reserves
      this job. If the worker does not delete, release, or bury the job within
      `:ttr` seconds, the job will time out and the server will release the job.
      The minimum ttr is 1. If the client sends 0, the server will silently
      increase the ttr to 1.
  """
  @spec put(String.t) :: result
  @spec put(String.t, [{:pri, integer}, {:delay, integer}, {:ttr, integer}]) :: result
  def put(data, opts \\ []) do
    {pool_name, opts} = opts_pool_name(opts)
    command(pool_name, {:put, data, opts})
  end
  def put!(data, opts \\ []) do
    case put(data, opts) do
      {:ok, job_id} -> job_id
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Put a job in the specified tube.
  The opts are the same as `put`
  """
  @spec put_in_tube(String.t, String.t) :: result
  @spec put_in_tube(String.t, String.t, [{:pri, integer}, {:delay, integer}, {:ttr, integer}]) :: result
  def put_in_tube(tube, data, opts \\ []) do
    {pool_name, opts} = opts_pool_name(opts)
    case pipeline(pool_name, [{:use, tube}, {:put, data, opts}]) do
      [{:ok, ^tube}, result] -> result
      error -> {:error, "#{inspect error}"}
    end
  end
  def put_in_tube!(tube, data, opts \\ []) do
    case put_in_tube(tube, data, opts) do
      {:ok, job_id} -> job_id
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Use a tube to `put` jobs.
  """
  @spec use(String.t, []) :: {:using, String.t}
  def use(tube, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:use, tube})
  end

  @doc """
  Add the named tube to the watch list for the current connection.
  A reserve command will take a job from any of the tubes in the
  watch list.
  """
  @spec watch(String.t, []) :: {:watching, non_neg_integer}
  def watch(tube, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:watch, tube})
  end

  @doc """
  Remove the named tube from the watch list for the current connection.
  """
  @spec ignore(String.t, []) :: {:watching, non_neg_integer} | :not_ignored
  def ignore(tube, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:ignore, tube})
  end

  @doc """
  Remove a job from the server entirely. It is normally used
  by the client when the job has successfully run to completion. A client can
  delete jobs that it has reserved, ready jobs, delayed jobs, and jobs that are
  buried.
  """
  @spec delete(non_neg_integer) :: :deleted | :not_found
  def delete(id, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:delete, id})
  end
  def delete!(id, opts \\ []) do
    case delete(id, opts) do
      {:ok, :deleted} -> :deleted
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Allow a worker to request more time to work on a job.
  This is useful for jobs that potentially take a long time, but you still want
  the benefits of a TTR pulling a job away from an unresponsive worker.  A worker
  may periodically tell the server that it's still alive and processing a job
  (e.g. it may do this on DEADLINE_SOON). The command postpones the auto
  release of a reserved job until TTR seconds from when the command is issued.
  """
  @spec touch(non_neg_integer) :: :touched | :not_found
  def touch(id, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:touch, id})
  end

  @doc """
  Let the client inspect a job in the system. Peeking the given job id
  """
  @spec peek(non_neg_integer) :: {:found, non_neg_integer} | :not_found
  def peek(id, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:peek, id})
  end

  @doc """
  Peeking the next ready job.
  """
  @spec peek_ready([]) :: {:found, non_neg_integer} | :not_found
  def peek_ready(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, :peek_ready)
  end

  @doc """
  Peeking the delayed job with the shortest delay left.
  """
  @spec peek_delayed([]) :: {:found, non_neg_integer} | :not_found
  def peek_delayed(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, :peek_delayed)
  end

  @doc """
  Peeking the next job in the list of buried jobs.
  """
  @spec peek_buried([]) :: {:found, non_neg_integer} | :not_found
  def peek_buried(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, :peek_buried)
  end

  @doc """
  Move jobs into the ready queue. If there are any buried jobs, it will only kick buried jobs.
  Otherwise it will kick delayed jobs.
  Apply only to the currently used tube.
  """
  @spec kick(non_neg_integer) :: {:kicked, non_neg_integer}
  def kick(bound, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:kick, bound})
  end

  @doc """
  Similar to `kick(bound)`, if the given job id exists and is in a buried or
  delayed state, it will be moved to the ready queue of the the same tube where it
  currently belongs.
  """
  @spec kick_job(non_neg_integer) :: :kicked | :not_found
  def kick_job(id, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:kick_job, id})
  end

  @doc """
  Return a list of all existing tubes in the server.
  """
  @spec list_tubes([]) :: list
  def list_tubes(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, :list_tubes)
  end

  @doc """
  Return the tube currently being used by the client.
  """

  @spec list_tube_used([]) :: {:using, String.t}
  def list_tube_used(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, :list_tube_used)
  end

  @doc """
  Return the tubes currently being watched by the client.
  """

  @spec list_tubes_watched([]) :: list
  def list_tubes_watched(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, :list_tubes_watched)
  end

  @doc """
  Get a job from the currently watched tubes.
  The opts can contain
  * `:timeout` - timeout in seconds or :infinity (default);
  """
  @spec reserve([]) :: {:reserved, non_neg_integer, String.t}
  def reserve(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:reserve, opts})
  end
  def reserve!(opts \\ []) do
    case reserve(opts) do
      {:ok, {job_id, data}} -> {job_id, data}
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Put a job into the "buried" state. Buried jobs are put into a
  FIFO linked list and will not be touched by the server again until a client
  kicks them with the `kick` command.
  """
  @spec bury(non_neg_integer) :: :buried | :not_found
  @spec bury(non_neg_integer, non_neg_integer) :: :buried | :not_found
  def bury(id, opts \\ []) do
    {pool_name, opts} = opts_pool_name(opts)
    command(pool_name, {:bury, id, opts})
  end

  @doc """
  Delay any new job being reserved for a given time.
  The opts can any combination of
  * `:delay` - an integer number of seconds to wait before putting the job back in the ready queue.
    The job will be in the "delayed" state during this time.
  """
  @spec pause_tube(String.t, non_neg_integer) :: :paused | :not_found
  def pause_tube(tube, opts \\ []) do
    {pool_name, opts} = opts_pool_name(opts)
    command(pool_name, {:pause_tube, tube, opts})
  end

  @doc """
  Put a reserved job back into the ready queue (and marks its state as "ready")
  to be run by any client. It is normally used when the job fails because of a transitory error.
  The opts can any combination of
  * `:pri` - a new priority to assign to the job;
  * `:delay` - an integer number of seconds to wait before putting the job back in the ready queue.
    The job will be in the "delayed" state during this time.
  """
  @spec release(non_neg_integer) :: :released | :buried | :not_found
  @spec release(non_neg_integer, [{:pri, integer}, {:delay, integer}]) :: :released | :buried | :not_found
  def release(id, opts \\ []) do
    {pool_name, opts} = opts_pool_name(opts)
    command(pool_name, {:release, id, opts})
  end

  @doc """
  Give statistical information about the system as a whole.
  """
  @spec stats([]) :: Map.t
  def stats(opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, :stats)
  end

  @doc """
  Similar to `stats/0`, gives statistical information about the specified job if
  it exists.
  """
  @spec stats_job(non_neg_integer) :: Map.t | :not_found
  def stats_job(id, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:stats_job, id})
  end

  @doc """
  Similar to `stats/0`, gives statistical information about the specified tube
  if it exists.
  """
  @spec stats_tube(String.t, []) :: Map.t | :not_found
  def stats_tube(tube, opts \\ []) do
    {pool_name, _} = opts_pool_name(opts)
    command(pool_name, {:stats_tube, tube})
  end

end
