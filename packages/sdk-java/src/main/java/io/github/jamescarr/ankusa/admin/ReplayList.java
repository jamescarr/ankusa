package io.github.jamescarr.ankusa.admin;

import java.util.List;

/**
 * One page of replay jobs, as {@code GET /v1/replays} returns it.
 *
 * @param replays the jobs, newest first
 */
public record ReplayList(List<Replay> replays) {

  /** Treats a body without a {@code replays} list as an empty page. */
  public ReplayList {
    replays = replays == null ? List.of() : List.copyOf(replays);
  }
}
