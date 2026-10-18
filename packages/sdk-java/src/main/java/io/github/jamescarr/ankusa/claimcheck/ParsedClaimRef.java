package io.github.jamescarr.ankusa.claimcheck;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A claim-check reference, split into the parts a gateway request needs.
 *
 * @param tenantId the tenant the claim belongs to
 * @param claimId the claim's ULID
 * @param path the gateway path this reference is redeemed at; a signed (v2) reference's path ends
 *     in {@code ?exp=<exp_unix>&sig=<signature>}
 */
public record ParsedClaimRef(String tenantId, String claimId, String path) {

  /**
   * {@code urn:ankusa:claim:v1:} followed by a tenant id and a 26-character Crockford-base32 ULID.
   */
  private static final Pattern REF =
      Pattern.compile("urn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})");

  /**
   * {@code urn:ankusa:claim:v2:} followed by the same two segments, the expiry (a decimal of 1-12
   * digits, no leading zero) and the signature (43 unpadded base64url characters). The gateway
   * checks the signature; the SDK only carries it.
   */
  private static final Pattern REF_V2 =
      Pattern.compile(
          "urn:ankusa:claim:v2:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})"
              + ":([1-9][0-9]{0,11}):([A-Za-z0-9_-]{43})");

  /**
   * Parses a claim-check reference.
   *
   * @param ref the reference, exactly {@code urn:ankusa:claim:v1:<tenant_id>:<claim_id>} or {@code
   *     urn:ankusa:claim:v2:<tenant_id>:<claim_id>:<exp_unix>:<signature>}
   * @return its parts
   * @throws InvalidClaimRefError when {@code ref} is null or is not exactly one whole reference; a
   *     trailing newline is enough to fail it
   */
  public static ParsedClaimRef parse(String ref) {
    Matcher signed = ref == null ? null : REF_V2.matcher(ref);

    if (signed != null && signed.matches()) {
      String tenantId = Objects.requireNonNull(signed.group(1));
      String claimId = Objects.requireNonNull(signed.group(2));
      String proof = "?exp=" + signed.group(3) + "&sig=" + signed.group(4);

      return new ParsedClaimRef(tenantId, claimId, path(tenantId, claimId) + proof);
    }

    Matcher matcher = ref == null ? null : REF.matcher(ref);

    if (matcher == null || !matcher.matches()) {
      throw new InvalidClaimRefError("invalid claim-check ref: \"" + ref + "\"");
    }

    String tenantId = Objects.requireNonNull(matcher.group(1));
    String claimId = Objects.requireNonNull(matcher.group(2));

    return new ParsedClaimRef(tenantId, claimId, path(tenantId, claimId));
  }

  private static String path(String tenantId, String claimId) {
    return "/v1/claims/" + tenantId + "/" + claimId;
  }
}
