defmodule Ankusa.Redis.OptionsTest do
  use ExUnit.Case, async: true

  alias Ankusa.Redis.Options

  @url "rediss://ops:s3cr3t@cache:6380/2"
  @mfa {__MODULE__, :password, [:inst]}

  test "the URL's password becomes the MFA; everything else is kept" do
    opts = Options.start_opts(@url, @mfa)

    assert opts[:password] == @mfa
    assert opts[:host] == "cache"
    assert opts[:port] == 6380
    assert opts[:database] == 2
    assert opts[:username] == "ops"
    assert opts[:ssl] == true
    refute inspect(opts) =~ "s3cr3t"
    assert Options.password(@url) == "s3cr3t"
  end

  test "a URL without a password gets no password option" do
    opts = Options.start_opts("redis://cache:6379", @mfa)

    refute Keyword.has_key?(opts, :password)
    assert Options.password("redis://cache:6379") == nil
  end
end
