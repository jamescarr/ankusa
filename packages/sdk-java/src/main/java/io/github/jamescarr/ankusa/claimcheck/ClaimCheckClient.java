package io.github.jamescarr.ankusa.claimcheck;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.TransportResponse;
import io.github.jamescarr.ankusa.internal.HttpCore;
import io.github.jamescarr.ankusa.internal.Json;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Pattern;
import tools.jackson.core.JacksonException;

/**
 * Redeems claim-check references from an Ankusa claim-check gateway.
 *
 * <p>For a large hook, Ankusa stores the body in its claim-check store and delivers a reference
 * instead; the worker fetches the body from this gateway, which is the cheapest listener to expose
 * to workers because nothing here writes. Construct one per gateway and share it — a client holds
 * no mutable state and every method is safe to call from any thread.
 *
 * <pre>{@code
 * ClaimCheckClient claimCheck = new ClaimCheckClient("http://ankusa.example:4001");
 *
 * // claimRef and sha256 are the queue message's fields.
 * byte[] body = claimCheck.redeem(claimRef, sha256);
 * }</pre>
 *
 * <p>Every call is one request and no redirect is ever followed: a 3xx is an unavailable gateway.
 */
public final class ClaimCheckClient {

  /** The only digest this SDK accepts: lower-case hex SHA-256. */
  private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

  private final HttpCore http;

  /**
   * Creates a client against a claim-check gateway.
   *
   * @param baseUrl the gateway's base URL, e.g. {@code http://ankusa.example:4001}
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public ClaimCheckClient(String baseUrl) {
    this(baseUrl, ClientOptions.defaults());
  }

  /**
   * Creates a client with headers, a timeout, or a transport of its own.
   *
   * @param baseUrl the gateway's base URL
   * @param options what every request carries
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public ClaimCheckClient(String baseUrl, ClientOptions options) {
    this.http = new HttpCore(baseUrl, options);
  }

  /**
   * Fetches a claim and verifies it against the digest the delivery named.
   *
   * <p>The digest is checked before the bytes are returned, so a body that fails it is never handed
   * to the caller: a mismatch is evidence of a corrupted or tampered transfer.
   *
   * @param ref the reference, exactly as the delivery carried it
   * @param sha256 the expected lower-case hex SHA-256 of the body
   * @return the claim's bytes, verified against {@code sha256}
   * @throws InvalidClaimRefError when {@code ref} is not a claim-check reference, or {@code sha256}
   *     is not 64 lower-case hex characters; nothing is sent
   * @throws ClaimNotFoundError when the gateway has no such claim
   * @throws ClaimRejectedError when the gateway refuses the redemption with another 4xx (not 408 or
   *     429)
   * @throws ClaimIntegrityError when the returned bytes do not match {@code sha256}
   * @throws ClaimCheckUnavailableError when the gateway is unreachable, answers 408, 429 or
   *     anything else other than 200/404/4xx, or the request times out
   */
  public byte[] redeem(String ref, String sha256) {
    ParsedClaimRef parsed = ParsedClaimRef.parse(ref);

    if (sha256 == null || !SHA256.matcher(sha256).matches()) {
      throw new InvalidClaimRefError("invalid claim sha256: \"" + sha256 + "\"");
    }

    TransportResponse response = get(parsed.path());

    if (response.status() == 200) {
      String actual = sha256Hex(response.body());
      if (!actual.equals(sha256)) {
        throw new ClaimIntegrityError(parsed.tenantId(), parsed.claimId());
      }
      return response.body();
    }

    if (response.status() == 404) {
      throw new ClaimNotFoundError(parsed.tenantId(), parsed.claimId());
    }

    // A gateway (or a proxy in front of it) that is throttling or timing out is telling the caller
    // to come back, not that the claim is gone.
    boolean busy = response.status() == 408 || response.status() == 429;
    if (!busy && response.status() >= 400 && response.status() <= 499) {
      throw new ClaimRejectedError(response.status(), Json.errorBody(response.body()));
    }

    throw new ClaimCheckUnavailableError(
        "claim-check gateway error (" + response.status() + ")", null);
  }

  /**
   * Probes the gateway.
   *
   * <p>A liveness check, not a readiness one: it says the gateway answers, and nothing about
   * whether any particular claim is stored.
   *
   * @return the gateway's health answer
   * @throws ClaimCheckUnavailableError when the gateway is unreachable, answers anything but 200,
   *     or answers with a body that is not the health object
   */
  public ClaimCheckHealth health() {
    TransportResponse response = get("/health");

    if (response.status() != 200) {
      throw new ClaimCheckUnavailableError(
          "claim-check gateway health check failed (" + response.status() + ")", null);
    }

    try {
      return Json.read(response.body(), ClaimCheckHealth.class);
    } catch (JacksonException | NullPointerException e) {
      throw new ClaimCheckUnavailableError(
          "claim-check gateway health check returned a non-JSON body (200)", e);
    }
  }

  /** Sends one GET, turning every transport failure into an unavailable gateway. */
  private TransportResponse get(String path) {
    try {
      return http.send("GET", path, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ClaimCheckUnavailableError("claim-check gateway request interrupted", e);
    } catch (IOException e) {
      throw new ClaimCheckUnavailableError("claim-check gateway unreachable", e);
    }
  }

  private static String sha256Hex(byte[] body) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the Java platform", e);
    }
  }
}
