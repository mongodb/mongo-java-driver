# Testing Binary subtype 9: Vector

The JSON files in this directory tree are platform-independent tests that drivers can use to prove their conformance to
the specification.

These tests focus on the roundtrip of the list numbers as input/output, along with their data type and byte padding.

Additional tests exist in `bson_corpus/tests/binary.json` but do not sufficiently test the end-to-end process of Vector
to BSON. For this reason, drivers must create a bespoke test runner for the vector subtype.

Each test case here pertains to a single vector. The inputs required to create the Binary BSON object are defined, and
when valid, the Canonical BSON and Extended JSON representations are included for comparison.

## Version

Files in the "specifications" repository have no version scheme. They are not tied to a MongoDB server version.

## Format

#### Top level keys

Each JSON file contains three top-level keys.

- `description`: human-readable description of what is in the file
- `test_key`: Field name used when decoding/encoding a BSON document containing the single BSON Binary for the test
  case. Applies to *every* case.
- `tests`: array of test case objects, each of which have the following keys. Valid cases will also contain additional
  binary and json encoding values.

#### Keys of tests objects

- `description`: string describing the test.
- `valid`: boolean indicating if the vector, dtype, and padding should be considered a valid input.
- `vector`: list of numbers
- `dtype_hex`: string defining the data type in hex (e.g. "0x10", "0x27")
- `dtype_alias`: (optional) string defining the data dtype, perhaps as Enum.
- `padding`: (optional) integer for byte padding. Defaults to 0.
- `canonical_bson`: (required if valid is true) an (uppercase) big-endian hex representation of a BSON byte string.
- `canonical_extjson`: (required if valid is true) string containing a Canonical Extended JSON document. Because this is
  itself embedded as a *string* inside a JSON document, characters like quote and backslash are escaped.