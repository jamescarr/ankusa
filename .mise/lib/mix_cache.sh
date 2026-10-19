# Shared compiled-deps cache for one Mix project and one MIX_ENV.
#
# Sourced, not run. `.mise/tasks/build` and `.mise/tasks/check/*` use it so a
# fresh worktree restores deps/ and _build/<env>/lib/* instead of compiling
# every Hex dep from source — RocksDB's C++ NIF alone is ~80 s on this Mac and
# ~8 min on a CI runner — once per project and once per env.
#
#   mix_cache_warm <dir> <label> <env>
#
#     <dir>    project directory, relative to $ROOT (e.g. packages/ankusa)
#     <label>  cache entry name (package name, or the example's directory)
#     <env>    MIX_ENV to warm; exported, and left set on return
#
# On return the shell is in $ROOT/<dir>, deps/ and _build/<env>/lib/* are
# populated, and every Hex dep is compiled. The project itself is not compiled:
# the caller's own `mix compile` / `mix test` / `hex.build` stays exactly as it
# was, so which warnings fail a build does not change.

# Keep one implementation of "restore, fetch the delta, compile only what is
# stale, save": `.mise/tasks/build` and the checks must agree on the cache key
# or each would miss the other's entries.
mix_cache_warm() {
  local dir=$1 label=$2 env=$3 lock=${4:-}
  local cache_name cache_root cache key tmp err present log stale n refetch i e os
  export MIX_ENV="$env"

  cd "$ROOT/$dir" || fail "no such project directory: $dir"

  # One cache entry per project, env, platform, toolchain, lock, and config:
  # config feeds compile_env, which Mix checks against what a dep was built
  # with.
  key=$(
    {
      printf '%s\n' "$label" "$env"
      uname -sm
      elixir -e 'IO.write([System.version(), "-otp", System.otp_release()])'
      cat mix.lock
      cat config/*.exs 2>/dev/null || true
    } | shasum -a 256 | cut -c1-16
  )
  # A label with a slash (an example's path) would nest the entry.
  cache_name=$(printf '%s' "$label" | tr '/' '-')
  cache_root="${ANKUSA_MIX_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/ankusa/mix}/$cache_name"
  cache="$cache_root/$key"

  tmp=""
  err=$(mktemp)

  # Copy-on-write where the filesystem has it (APFS clonefile, reflinks), so a
  # restore is near-instant and costs no disk until something is rewritten.
  # `-p`: Mix marks a dep stale when a source file is newer than the manifest
  # that compiled it, so restored files must keep the timestamps they were
  # built with (a plain `cp` would date them now and recompile every dep).
  os=$(uname -s)
  _mix_cache_clone() {
    if [ "$os" = Darwin ]; then
      cp -cRp "$1" "$2" 2>/dev/null || { rm -rf "$2"; cp -Rp "$1" "$2"; }
    else
      cp -Rp --reflink=auto "$1" "$2"
    fi
  }

  # Restore. Mix trusts a dep in _build when its .app and
  # .mix/compile.elixir_scm (Elixir/OTP version, SCM, lock entry) match, none
  # of which carry this checkout's path, so cloned directories need no
  # recompile. Existing entries are never overwritten: a local build in
  # progress wins.
  #
  # A NIF dep's compiled artifacts live in deps/<app>/priv (RocksDB's .so,
  # crc32cer's). If the dep directory here is source only — an earlier
  # `mix deps.get` with no compile — its beams are not restored: Mix would
  # trust them and skip the rebuild, and the app would fail to load.
  if [ -d "$cache" ]; then
    mkdir -p deps "_build/$env/lib"
    for src in "$cache"/deps/*; do
      [ -e "$src" ] || continue
      n="deps/${src##*/}"
      if [ ! -e "$n" ] && [ ! -L "$n" ]; then
        _mix_cache_clone "$src" "$n"
        # A rebar/cmake dep's own build tree (deps/<app>/_build) records the
        # absolute directory it was configured in, and CMake refuses to reuse
        # it from another worktree ("current CMakeCache.txt directory … is
        # different"). Drop what we just restored so a rebuild reconfigures
        # cleanly; entries saved before this was stripped still carry it.
        rm -rf "$n/_build"
        # Same for a CMake tree kept elsewhere (crc32cer uses c_src/build).
        find "$n" -name CMakeCache.txt -delete 2>/dev/null || true
      fi
    done
    for src in "$cache"/lib/*; do
      [ -e "$src" ] || continue
      app=${src##*/}
      if [ -e "deps/$app" ] && [ -d "$cache/deps/$app/priv" ] && [ ! -d "deps/$app/priv" ]; then
        continue
      fi
      n="_build/$env/lib/$app"
      [ -e "$n" ] || [ -L "$n" ] || _mix_cache_clone "$src" "$n"
    done
    touch "$cache"
    echo "==> deps restored from $cache"
  fi

  # What is in deps/ before the fetch; anything deps.get fetches now is
  # pristine.
  present=" "
  for n in deps/*; do
    if [ -e "$n" ]; then present="$present${n##*/} "; fi
  done

  # shellcheck disable=SC2086 # $lock is empty or --check-locked
  mix deps.get $lock

  log="_build/$env/deps-compile.log"
  mkdir -p "_build/$env"

  # The Hex deps Mix would build on its own: unbuilt, stale, or built by
  # another Elixir/OTP. Not a bare `mix deps.compile`, which reruns every dep's
  # build: that recompiles cloned Mix deps and breaks the CMake NIFs (rocksdb,
  # crc32cer), whose build trees record the absolute path they were built at.
  # Mix.Dep is Mix's internal API (the calls deps.loadpaths makes), pinned by
  # the Elixir in .mise.toml; a rename fails here, loudly. stderr is only shown
  # on failure: mix run warns about our app until its first build.
  _mix_cache_stale_deps() {
    mix run --no-start --no-compile --no-deps-check -e '
      for dep <- Mix.Dep.load_and_cache(),
          dep.scm.fetchable?(),
          Mix.Dep.compilable?(Mix.Dep.check_lock(dep)),
          do: IO.puts(dep.app)
    ' 2>"$err"
  }
  if stale=$(_mix_cache_stale_deps); then
    :
  else
    # A project whose config/runtime.exs needs an env var at this point (a
    # Phoenix example wants SECRET_KEY_BASE) cannot be probed: `mix run`
    # evaluates runtime config. Nothing is skipped — the caller's own compile
    # builds every dep, just without the cache's help.
    sed -n '1p' "$err" >&2 || true
    echo "==> could not list the Hex deps to build; leaving them to this project's own compile"
    stale=""
  fi

  # A stale dep that was already here may hold a build tree from another
  # checkout (cloned from the cache): rebuild it from a fresh fetch instead.
  # The fetch also marks every dep that depends on it stale, so list them
  # again.
  refetch=""
  for n in $stale; do
    case "$present" in *" $n "*)
      rm -rf "deps/$n"
      refetch=1
      ;;
    esac
  done
  if [ -n "$refetch" ]; then
    # shellcheck disable=SC2086 # $lock is empty or --check-locked
    mix deps.get $lock
    stale=$(_mix_cache_stale_deps) || stale=""
  fi

  if [ -z "$stale" ]; then
    echo "==> third-party deps up to date"
  else
    # Output to the log: their warnings are upstream's (every one is already
    # on its latest release). ERL_COMPILER_OPTIONS sits below each dep's own
    # -compile attributes and silences the Erlang lints that would otherwise
    # fill the log. Scoped to this command; it must not reach our compile.
    # shellcheck disable=SC2086 # $stale is a list of app names
    if ! ERL_COMPILER_OPTIONS='[nowarn_deprecated_catch,nowarn_match_alias_pats]' \
      MIX_OS_DEPS_COMPILE_PARTITION_COUNT="${MIX_OS_DEPS_COMPILE_PARTITION_COUNT:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}" \
      mix deps.compile $stale >"$log" 2>&1; then
      cat "$log" >&2
      rm -f "$err"
      fail "mix deps.compile failed (full log above, also in $dir/$log)"
    fi
    echo "==> third-party deps compiled (log: $dir/$log)"
  fi

  # Save. Built in a temp dir and moved into place, so a concurrent build in
  # another worktree never sees half an entry; if that build wins the race, mv
  # nests ours inside its entry, and the second rm drops it.
  if [ ! -d "$cache" ]; then
    tmp="$cache_root/.tmp.$$"
    mkdir -p "$tmp/lib"
    _mix_cache_clone deps "$tmp/deps"
    # Keep the entry path-independent: a dep's own CMake build tree names the
    # directory it was configured in and cannot be reused elsewhere.
    rm -rf "$tmp/deps"/*/_build
    for n in deps/*; do
      if [ -d "_build/$env/lib/${n##*/}" ]; then
        _mix_cache_clone "_build/$env/lib/${n##*/}" "$tmp/lib/${n##*/}"
      fi
    done
    if [ ! -e "$cache" ]; then mv "$tmp" "$cache"; else rm -rf "$tmp"; fi
    rm -rf "$cache/.tmp.$$"
    tmp=""
    echo "==> deps cached in $cache"
    # Keep the three most recently used entries (ls skips the .tmp.* dirs).
    i=0
    for e in $(ls -t "$cache_root"); do
      i=$((i + 1))
      if [ "$i" -gt 3 ]; then rm -rf "${cache_root:?}/$e"; fi
    done
  fi

  rm -f "$err"
}

# Dialyzer PLTs. `_build/plts` sits beside `_build/<env>/lib`, which the entry
# above restores, so without this every fresh worktree rebuilds the erts+Elixir
# PLT before its first analysis.
#
# Only the core PLT is shared: `dialyxir_erlang-<otp>_elixir-<elixir>.plt` holds
# erts and Elixir's own applications, whose beams are at the same absolute path
# in every worktree of this machine, and its filename encodes both versions, so
# a toolchain change names a different file rather than reusing an old one.
# The `*_deps-<env>.plt` beside it is this checkout's dependency graph (dialyxir
# pairs it with a `.hash`) and is never copied. Both are per project, not per
# MIX_ENV, so this is called around `mix dialyzer`, not from mix_cache_warm.
#
# Keyed by package as well as toolchain: `plt_add_apps` is a package setting
# (ankusa and async_api_spex add `:mix`) while the core PLT's filename carries
# only the toolchain versions, so one file would otherwise be handed to a
# package whose core PLT must contain more.
mix_plt_dir() {
  printf '%s/plts/%s/%s' "${ANKUSA_MIX_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/ankusa/mix}" \
    "$1" \
    "$(elixir -e 'IO.write([System.version(), "-otp", System.otp_release()])')"
}

mix_plt_restore() {
  local src plt
  src=$(mix_plt_dir "$1") || return 0
  [ -d "$src" ] || return 0
  mkdir -p _build/plts || return 0
  for plt in "$src"/dialyxir_erlang-*.plt; do
    [ -e "$plt" ] || continue
    [ -e "_build/plts/${plt##*/}" ] || cp -p "$plt" "_build/plts/" || return 0
  done
}

mix_plt_save() {
  local dst plt name
  [ -d _build/plts ] || return 0
  dst=$(mix_plt_dir "$1") || return 0
  mkdir -p "$dst" || return 0
  for plt in _build/plts/dialyxir_erlang-*.plt; do
    [ -e "$plt" ] || continue
    name=${plt##*/}
    case "$name" in *_deps-*) continue ;; esac
    [ -e "$dst/$name" ] && continue
    cp -p "$plt" "$dst/.tmp.$$.$name" || return 0
    mv "$dst/.tmp.$$.$name" "$dst/$name" || return 0
  done
  rm -f "$dst"/.tmp.$$.* 2>/dev/null || true
}
