defmodule Mix.Tasks.Ankusa.Wal.MigrateTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias Mix.Tasks.Ankusa.Wal.Migrate

  test "parses each comma-separated entry as a full node name" do
    assert Migrate.parse_members("ankusa@a,ankusa@b", "default") == [
             {:ankusa_wal_default, :ankusa@a},
             {:ankusa_wal_default, :ankusa@b}
           ]
  end
end
