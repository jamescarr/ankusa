# Corrupt 0.3 frames

Inputs to `Ankusa.Store.Migrate.run/1`, replayed by
`test/ankusa/property/corrupt_frames_test.exs`. Every file here must make the
import return `{:error, _}`: the node refuses to start rather than guess.

A torn *final* frame is not here: it is a write that was never acked, so it is
forgiven and the frames before it import (`migrate_property_test.exs` generates
those). What is here is damage with acked data after it, or a cursor that cannot
be trusted.

`*.wal` is a `wal/ankusa.wal` of three-or-fewer frames, `<<0x484B::16, 1::8,
flags::8, seq::64, crc32::32, len::32, payload>>`, the CRC covering the payload.
`*.cursors` and `*.truncated` are `wal/ankusa.wal.cursors` and
`wal/ankusa.wal.truncated` beside one valid frame.

| File | The damage |
| --- | --- |
| `bitflip_payload_then_valid.wal` | one bit flipped in frame 1's payload (CRC mismatch), frame 2 intact |
| `bitflip_crc_then_valid.wal` | one bit flipped in frame 1's CRC field |
| `bitflip_length_then_valid.wal` | one bit flipped in frame 1's length field |
| `bitflip_magic_then_valid.wal` | one bit flipped in frame 1's magic |
| `bitflip_version_then_valid.wal` | one bit flipped in frame 1's version byte |
| `zeroed_header_then_valid.wal` | frame 1's 20-byte header zeroed |
| `torn_frame_glued_to_next.wal` | frame 1 cut in half, frame 2 written straight after it |
| `zero_page_then_valid.wal` | 512 zero bytes between frames 1 and 2 |
| `zero_extended_header_then_valid.wal` | a header with length 0 and CRC 0 (a torn write the file was zero-extended past) then 64 zeros, between frames 1 and 2 |
| `garbage_run_then_valid.wal` | 37 bytes of `0xA5` between frames 1 and 2 |
| `damage_in_second_of_three.wal` | one payload bit flipped in frame 2 of 3 |
| `cursors_garbage.cursors` | not an Erlang term |
| `cursors_torn.cursors` | a valid cursor map missing its last 3 bytes |
| `cursors_wrong_shape.cursors` | a list, not a map |
| `cursors_negative.cursors` | `%{dispatch: -1, compactor: 0}` |
| `floor_negative.truncated` | the floor is `-5` |
| `floor_not_integer.truncated` | the floor is a string |

To add a case: write the file, add its name to `@expected` in the test, and
describe it above. The frames inside are built from fixed envelopes
(`id` `00000000-0000-7000-8000-00000000000<seq>`, `received_at` 1790258591000 +
`seq`), so a file is reproducible byte for byte.
