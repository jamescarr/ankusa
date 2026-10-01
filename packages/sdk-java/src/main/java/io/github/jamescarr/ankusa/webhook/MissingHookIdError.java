package io.github.jamescarr.ankusa.webhook;

import io.github.jamescarr.ankusa.AnkusaException;

/**
 * An HTTP delivery arrived without an {@code x-ankusa-id} header.
 *
 * <p>Every Ankusa delivery carries one, so its absence means whatever reached this receiver did not
 * come from Ankusa. Answer 400 and fail the delivery: there is no id to deduplicate on.
 */
public final class MissingHookIdError extends AnkusaException {

  /** Creates the error. */
  public MissingHookIdError() {
    super("missing x-ankusa-id header");
  }
}
