(ns ankusa.sdk.impl.digest
  "The SHA-256 every integrity check compares: lowercase hex, 64 characters.")

(set! *warn-on-reflection* true)

(defn sha256-hex
  "Lowercase hex SHA-256 of `bytes`."
  [^bytes bytes]
  (let [digest (.digest (java.security.MessageDigest/getInstance "SHA-256") bytes)]
    (format "%064x" (BigInteger. 1 ^bytes digest))))
