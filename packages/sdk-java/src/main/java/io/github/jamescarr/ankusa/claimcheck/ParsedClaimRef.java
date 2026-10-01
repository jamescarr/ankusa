package io.github.jamescarr.ankusa.claimcheck;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A claim-check reference, split into the parts a gateway request needs.
 *
 * @param tenantId the tenant the claim belongs to
 * @param claimId the claim's ULID
 * @param path the gateway path this reference is redeemed at
 */
public record ParsedClaimRef(String tenantId, String claimId, String path) {

  /**
   * {@code urn:ankusa:claim:v1:} followed by a tenant id and a 26-character Crockford-base32 ULID.
   */
  private static final Pattern REF =
      Pattern.compile("urn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})");

  /**
   * Parses a claim-check reference.
   *
   * @param ref the reference, exactly {@code urn:ankusa:claim:v1:<tenant_id>:<claim_id>}
   * @return its parts
   * @throws InvalidClaimRefError when {@code ref} is null or is not exactly one whole reference; a
   *     trailing newline is enough to fail it
   */
  public static ParsedClaimRef parse(String ref) {
    Matcher matcher = ref == null ? null : REF.matcher(ref);

    if (matcher == null || !matcher.matches()) {
      throw new InvalidClaimRefError("invalid claim-check ref: \"" + ref + "\"");
    }

    String tenantId = Objects.requireNonNull(matcher.group(1));
    String claimId = Objects.requireNonNull(matcher.group(2));

    return new ParsedClaimRef(tenantId, claimId, "/v1/claims/" + tenantId + "/" + claimId);
  }
}
