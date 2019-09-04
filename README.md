# Beanstix

![Travis](https://travis-ci.org/nicksanders/beanstix.svg?branch=master)

**A beanstalkd client for Elixir**

Mostly a fork of elixir_talk without the yaml dependency and with the added ability to send multiple commands.

All commands return tuples with :ok or :error

Warning: This is Alpha software and subject to breaking changes.
<!--
## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `beanstix` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:beanstix, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/beanstix](https://hexdocs.pm/beanstix).
-->

## Getting Started
    iex -S mix
    iex(1)> host = '127.0.0.1'
    iex(1)> port = 11300
    iex(1)> {:ok, pid} = Beanstix.connect(host, port)

Host and Port default to '127.0.0.1' and 11300

## Basic Operation
After connection to the beanstalkd successfully, we can enqueue our jobs:

    iex(2)> Beanstix.put(pid, "hello world")
    {:ok, 352}

Or we can get jobs:

    iex(3)> Beanstix.reserve(pid)
    {:ok, {1, "hello world"}}

Once we are finishing a job, we have to delete it, otherwise jobs are re-queued by **beanstalkd**
after a `:ttr` "time to run" (60 seconds, per default) is surpassed. A job is marked as finished, by calling delete:

    iex(4)> Beanstix.delete(pid, 1)
    {:ok, :deleted}

`reserve` blocks until a job is ready, possibly forever. We can invoke reserve with a timeout **in seconds**,
to indicate how long we want to wait to receive a job. If such a reserve times out, it will return `:timed_out`:

    iex(12)> Beanstix.reserve(pid, 2)
    {:ok, :timed_out}

If you use a timeout of 0, reserve will immediately return either a job or `:timed_out`.

## Tube Management

A single **beanstalkd** server can provide many different queues, called "tubes" in **beanstalkd**.
To see all available tubes:

    iex(6)> Beanstix.list_tubes(pid)
    {:ok, ["default"]}


## How to access beanstalkd from command line

  ```
  socat - tcp4-connect:0.0.0.0:11300,crnl
  ```
