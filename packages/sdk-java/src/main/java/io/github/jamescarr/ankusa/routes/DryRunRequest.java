package io.github.jamescarr.ankusa.routes;

import java.util.Objects;

/**
 * A request to run against the route table without capturing anything.
 *
 * @param method the HTTP method of the request
 * @param path the path of the request
 * @param ip the client address the request would come from
 */
public record DryRunRequest(String method, String path, String ip) {

  /**
   * Validates the components.
   *
   * @throws NullPointerException when any component is null
   */
  public DryRunRequest {
    Objects.requireNonNull(method, "method");
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(ip, "ip");
  }
}
