package io.github.jamescarr.ankusa.webhook;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.jspecify.annotations.Nullable;

/**
 * Verifies the <a href="https://www.standardwebhooks.com/">Standard Webhooks</a> signature an HTTP
 * sink with a {@code secret} adds.
 *
 * <p>{@code webhook-signature} holds space-separated {@code v1,<base64>} entries, each an
 * HMAC-SHA256 over {@code <webhook-id>.<webhook-timestamp>.<body>}. A delivery passes when any
 * {@code v1} entry matches any secret (several during a rotation), compared in constant time with
 * {@link MessageDigest#isEqual}, and {@code webhook-timestamp} is within the tolerance of now.
 * Secrets are {@code whsec_} + base64, or any other string used as its own UTF-8 bytes.
 */
public final class Signature {

  /** The default {@code webhook-timestamp} window, either side of now. */
  public static final Duration DEFAULT_TOLERANCE = Duration.ofMinutes(5);

  private static final Pattern DIGITS = Pattern.compile("[0-9]+");

  private Signature() {}

  /**
   * A verified delivery's identity.
   *
   * @param id the {@code webhook-id}
   * @param timestamp the {@code webhook-timestamp}, unix seconds
   */
  public record Verified(String id, long timestamp) {}

  /**
   * Verifies one delivery against the clock with the default tolerance.
   *
   * @param headers the request's headers, names in any case
   * @param body the raw body, exactly as received
   * @param secrets the configured secrets
   * @return the verified id and timestamp
   * @throws InvalidSignatureError when the delivery does not verify
   */
  public static Verified verify(
      Map<String, ? extends List<String>> headers, byte[] body, List<String> secrets) {
    return verify(headers, body, secrets, DEFAULT_TOLERANCE, Instant.now());
  }

  /**
   * Verifies one delivery.
   *
   * @param headers the request's headers, names in any case
   * @param body the raw body, exactly as received
   * @param secrets the configured secrets
   * @param tolerance how far either side of {@code now} the timestamp may be
   * @param now the instant to judge the timestamp against
   * @return the verified id and timestamp
   * @throws InvalidSignatureError when the delivery does not verify
   */
  public static Verified verify(
      Map<String, ? extends List<String>> headers,
      byte[] body,
      List<String> secrets,
      Duration tolerance,
      Instant now) {
    return verify(
        name -> {
          for (Map.Entry<String, ? extends List<String>> entry : headers.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            if (key != null && key.equalsIgnoreCase(name) && values != null && !values.isEmpty()) {
              return values.get(0);
            }
          }
          return null;
        },
        body,
        secrets,
        tolerance,
        now);
  }

  /**
   * Verifies one delivery, reading headers through a case-insensitive lookup such as {@code
   * HttpServletRequest::getHeader}.
   *
   * <p>Checks run in this order: {@code invalid_secret}, {@code missing_header} ({@code
   * webhook-id}, {@code webhook-timestamp}, {@code webhook-signature}), {@code invalid_timestamp},
   * {@code timestamp_out_of_tolerance}, {@code no_matching_signature}.
   *
   * @param lookup returns a header's value, or null when it is absent
   * @param body the raw body, exactly as received
   * @param secrets the configured secrets
   * @param tolerance how far either side of {@code now} the timestamp may be
   * @param now the instant to judge the timestamp against
   * @return the verified id and timestamp
   * @throws InvalidSignatureError when the delivery does not verify
   */
  public static Verified verify(
      Function<String, @Nullable String> lookup,
      byte[] body,
      List<String> secrets,
      Duration tolerance,
      Instant now) {
    List<byte[]> keys = keys(secrets);

    String id = required(lookup, "webhook-id");
    String rawTimestamp = required(lookup, "webhook-timestamp");
    String signature = required(lookup, "webhook-signature");

    long timestamp;
    try {
      if (!DIGITS.matcher(rawTimestamp).matches()) {
        throw new NumberFormatException(rawTimestamp);
      }
      timestamp = Long.parseLong(rawTimestamp);
    } catch (NumberFormatException e) {
      throw new InvalidSignatureError(
          "invalid_timestamp", "webhook-timestamp", "webhook-timestamp is not a unix time");
    }

    if (Math.abs(now.getEpochSecond() - timestamp) > tolerance.toSeconds()) {
      throw new InvalidSignatureError(
          "timestamp_out_of_tolerance",
          "webhook-timestamp",
          "webhook-timestamp is outside the tolerance window");
    }

    List<byte[]> candidates = new ArrayList<>();
    for (String entry : signature.split(" ", -1)) {
      if (entry.startsWith("v1,")) {
        // Not base64: it cannot match, and another entry still might.
        byte[] candidate = strictBase64(entry.substring(3));
        if (candidate != null) {
          candidates.add(candidate);
        }
      }
    }

    byte[] prefix = (id + "." + rawTimestamp + ".").getBytes(StandardCharsets.UTF_8);
    for (byte[] key : keys) {
      byte[] expected = hmac(key, prefix, body);
      for (byte[] candidate : candidates) {
        if (MessageDigest.isEqual(expected, candidate)) {
          return new Verified(id, timestamp);
        }
      }
    }

    throw new InvalidSignatureError(
        "no_matching_signature", "webhook-signature", "no webhook-signature entry matches");
  }

  private static String required(Function<String, @Nullable String> lookup, String name) {
    String value = lookup.apply(name);
    if (value == null || value.isEmpty()) {
      throw new InvalidSignatureError("missing_header", name, "missing " + name + " header");
    }
    return value;
  }

  /**
   * Padded base64 only, as core and every other SDK decode it: the JDK decoder alone also takes
   * unpadded input.
   */
  private static byte @Nullable [] strictBase64(String encoded) {
    if (encoded.length() % 4 != 0) {
      return null;
    }
    try {
      return Base64.getDecoder().decode(encoded);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static List<byte[]> keys(List<String> secrets) {
    if (secrets.isEmpty()) {
      throw new InvalidSignatureError("invalid_secret", null, "no secret configured");
    }

    List<byte[]> keys = new ArrayList<>(secrets.size());
    for (String secret : secrets) {
      if (secret.startsWith("whsec_")) {
        byte[] key = strictBase64(secret.substring(6));
        if (key == null) {
          key = new byte[0];
        }
        if (key.length == 0) {
          throw new InvalidSignatureError(
              "invalid_secret", null, "a whsec_ secret is not valid base64");
        }
        keys.add(key);
      } else if (secret.isEmpty()) {
        throw new InvalidSignatureError("invalid_secret", null, "an empty secret");
      } else {
        keys.add(secret.getBytes(StandardCharsets.UTF_8));
      }
    }
    return keys;
  }

  private static byte[] hmac(byte[] key, byte[] prefix, byte[] body) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      mac.update(prefix);
      return mac.doFinal(body);
    } catch (GeneralSecurityException e) {
      // Every JDK ships HmacSHA256, and a non-empty key is always valid for it.
      throw new IllegalStateException("HmacSHA256 is unavailable", e);
    }
  }
}
