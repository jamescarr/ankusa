defmodule Ankusa.Test.OCIKey do
  @moduledoc """
  OCI's own published test key and signing string, from the "Request Signatures"
  page (https://docs.oracle.com/iaas/Content/API/Concepts/signingrequests.htm).
  The key is RSA-2048 and is documented there as test-only.

  `reference_signature/0` is the RSA-SHA256 (PKCS#1 v1.5) signature of
  `reference_signing_string/0` under `private_key/0`, computed independently
  with OpenSSL (`openssl dgst -sha256 -sign`), so the signing test pins OTP's
  `:public_key.sign/3` against an implementation outside the BEAM.
  """

  @pem """
  -----BEGIN RSA PRIVATE KEY-----
  MIICXgIBAAKBgQDCFENGw33yGihy92pDjZQhl0C36rPJj+CvfSC8+q28hxA161QF
  NUd13wuCTUcq0Qd2qsBe/2hFyc2DCJJg0h1L78+6Z4UMR7EOcpfdUE9Hf3m/hs+F
  UR45uBJeDK1HSFHD8bHKD6kv8FPGfJTotc+2xjJwoYi+1hqp1fIekaxsyQIDAQAB
  AoGBAJR8ZkCUvx5kzv+utdl7T5MnordT1TvoXXJGXK7ZZ+UuvMNUCdN2QPc4sBiA
  QWvLw1cSKt5DsKZ8UETpYPy8pPYnnDEz2dDYiaew9+xEpubyeW2oH4Zx71wqBtOK
  kqwrXa/pzdpiucRRjk6vE6YY7EBBs/g7uanVpGibOVAEsqH1AkEA7DkjVH28WDUg
  f1nqvfn2Kj6CT7nIcE3jGJsZZ7zlZmBmHFDONMLUrXR/Zm3pR5m0tCmBqa5RK95u
  412jt1dPIwJBANJT3v8pnkth48bQo/fKel6uEYyboRtA5/uHuHkZ6FQF7OUkGogc
  mSJluOdc5t6hI1VsLn0QZEjQZMEOWr+wKSMCQQCC4kXJEsHAve77oP6HtG/IiEn7
  kpyUXRNvFsDE0czpJJBvL/aRFUJxuRK91jhjC68sA7NsKMGg5OXb5I5Jj36xAkEA
  gIT7aFOYBFwGgQAQkWNKLvySgKbAZRTeLBacpHMuQdl1DfdntvAyqpAZ0lY0RKmW
  G6aFKaqQfOXKCyWoUiVknQJAXrlgySFci/2ueKlIE1QqIiLSZ8V8OlpFLRnb1pzI
  7U1yQXnTAEFYM560yJlzUpOb1V4cScGd365tiSMvxLOvTA==
  -----END RSA PRIVATE KEY-----
  """

  @signing_string """
  date: Thu, 05 Jan 2014 21:31:40 GMT
  (request-target): get /20160918/instances?availabilityDomain=Pjwf%3A%20PHX-AD-1&compartmentId=ocid1.compartment.oc1..aaaaaaaam3we6vgnherjq5q2idnccdflvjsnog7mlr6rtdb25gilchfeyjxa&displayName=TeamXInstances&volumeId=ocid1.volume.oc1.phx.abyhqljrgvttnlx73nmrwfaux7kcvzfs3s66izvxf2h4lgvyndsdsnoiwr5q
  host: iaas.us-phoenix-1.oraclecloud.com
  """

  @signature "VzdZIk+b+KkAnYFZ31RNW/qD4TXy8Nj3MPBH/glR6Fle3MoCDZGGRzDjRVmVaA38F/EHnQVpicITTn6eAdYdkT9JvupuTI8P0NERCllTrQSITbIt+F69V6abRWStqQtQN/SfvJn4PHSp3UZS6e0EYZh4etqytjLF0qq2oANBguQ="

  def private_key_pem, do: @pem

  def private_key do
    [entry] = :public_key.pem_decode(@pem)
    :public_key.pem_entry_decode(entry)
  end

  def signing_string, do: @signing_string
  def reference_signature, do: @signature
end
