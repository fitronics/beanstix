# Beanstix

**A beanstalkd client for Elixir**

Initial implementation based on code from elixir_talk and redix

Warning: This is Alpha software and subject to breaking changes.

## Installation

The package is not available in hex yet

<!--
If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `beanstix` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:beanstix, "~> 0.1.0"}]
    end
    ```

  2. Ensure `beanstix` is started before your application:

    ```elixir
    def application do
      [applications: [:beanstix]]
    end
    ```
-->
## Benchmarking

```sh-session
$ mix deps.get
$ MIX_ENV=bench mix compile
$ MIX_ENV=bench mix bench
```

## How to access beanstalkd from command line

  ```
  socat - tcp4-connect:0.0.0.0:11300,crnl
  ```

 ## TODO

 1. ~~Port gen_tcp implementation to the way redix works with active: :once~~
 1. ~~Add support for pipelining probably by copying redix~~
 1. Add dialyzir specs
 1. Add documention for real world usage and also on how to use GenStage to consume?
