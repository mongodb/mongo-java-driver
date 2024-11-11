# Initial DNS Seedlist Discovery tests

This directory contains platform-independent tests that drivers can use to prove their conformance to the Initial DNS
Seedlist Discovery spec.

## Prose Tests

For the following prose tests, it is assumed drivers are be able to stub DNS results to easily test invalid DNS
resolution results.

### 1. Allow SRVs with fewer than 3 `.` separated parts

When running validation on an SRV string before DNS resolution, do not throw a error due to number of SRV parts.

- `mongodb+srv://localhost`
- `mongodb+srv://mongo.local`

### 2. Throw when return address does not end with SRV domain

When given a returned address that does NOT end with the original SRV's domain name, throw a runtime error.

For this test, run each of the following cases:

- the SRV `mongodb+srv://localhost` resolving to `localhost.mongodb`
- the SRV `mongodb+srv://mongo.local` resolving to `test_1.evil.local`
- the SRV `mongodb+srv://blogs.mongodb.com` resolving to `blogs.evil.com`

Remember, the domain of an SRV with one or two `.` separated parts is the SRVs entire hostname.

### 3. Throw when return address is identical to SRV hostname

When given a returned address that is identical to the SRV hostname and the SRV hostname has fewer than three `.`
separated parts, throw a runtime error.

For this test, run each of the following cases:

- the SRV `mongodb+srv://localhost` resolving to `localhost`
- the SRV `mongodb+srv://mongo.local` resolving to `mongo.local`

### 4. Throw when return address does not contain `.` separating shared part of domain

When given a returned address that does NOT share the domain name of the SRV record because it's missing a `.`, throw a
runtime error.

For this test, run each of the following cases:

- the SRV `mongodb+srv://localhost` resolving to `test_1.cluster_1localhost`
- the SRV `mongodb+srv://mongo.local` resolving to `test_1.my_hostmongo.local`
- the SRV `mongodb+srv://blogs.mongodb.com` resolving to `cluster.testmongodb.com`

## Test Setup

The tests in the `replica-set` directory MUST be executed against a three-node replica set on localhost ports 27017,
27018, and 27019 with replica set name `repl0`.

The tests in the `load-balanced` directory MUST be executed against a load-balanced sharded cluster with the mongos
servers running on localhost ports 27017 and 27018 and `--loadBalancerPort` 27050 and 27051, respectively (corresponding
to the script in
[drivers-evergreen-tools](https://github.com/mongodb-labs/drivers-evergreen-tools/blob/master/.evergreen/run-load-balancer.sh)).
The load balancers, shard servers, and config servers may run on any open ports.

The tests in the `sharded` directory MUST be executed against a sharded cluster with the mongos servers running on
localhost ports 27017 and 27018. Shard servers and config servers may run on any open ports.

In all cases, the clusters MUST be started with SSL enabled.

To run the tests that accompany this spec, you need to configure the SRV and TXT records with a real name server. The
following records are required for these tests:

```
Record                                    TTL    Class   Address
localhost.test.build.10gen.cc.            86400  IN A    127.0.0.1
localhost.sub.test.build.10gen.cc.        86400  IN A    127.0.0.1

Record                                      TTL    Class   Port   Target
_mongodb._tcp.test1.test.build.10gen.cc.    86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test1.test.build.10gen.cc.    86400  IN SRV  27018  localhost.test.build.10gen.cc.
_mongodb._tcp.test2.test.build.10gen.cc.    86400  IN SRV  27018  localhost.test.build.10gen.cc.
_mongodb._tcp.test2.test.build.10gen.cc.    86400  IN SRV  27019  localhost.test.build.10gen.cc.
_mongodb._tcp.test3.test.build.10gen.cc.    86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test5.test.build.10gen.cc.    86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test6.test.build.10gen.cc.    86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test7.test.build.10gen.cc.    86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test8.test.build.10gen.cc.    86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test10.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test11.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test12.test.build.10gen.cc.   86400  IN SRV  27017  localhost.build.10gen.cc.
_mongodb._tcp.test13.test.build.10gen.cc.   86400  IN SRV  27017  test.build.10gen.cc.
_mongodb._tcp.test14.test.build.10gen.cc.   86400  IN SRV  27017  localhost.not-test.build.10gen.cc.
_mongodb._tcp.test15.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.not-build.10gen.cc.
_mongodb._tcp.test16.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.build.not-10gen.cc.
_mongodb._tcp.test17.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.build.10gen.not-cc.
_mongodb._tcp.test18.test.build.10gen.cc.   86400  IN SRV  27017  localhost.sub.test.build.10gen.cc.
_mongodb._tcp.test19.test.build.10gen.cc.   86400  IN SRV  27017  localhost.evil.build.10gen.cc.
_mongodb._tcp.test19.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test20.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test21.test.build.10gen.cc.   86400  IN SRV  27017  localhost.test.build.10gen.cc.
_customname._tcp.test22.test.build.10gen.cc 86400  IN SRV  27017  localhost.test.build.10gen.cc.
_mongodb._tcp.test23.test.build.10gen.cc.   86400  IN SRV  8000   localhost.test.build.10gen.cc.
_mongodb._tcp.test24.test.build.10gen.cc.   86400  IN SRV  8000   localhost.test.build.10gen.cc.

Record                                    TTL    Class   Text
test5.test.build.10gen.cc.                86400  IN TXT  "replicaSet=repl0&authSource=thisDB"
test6.test.build.10gen.cc.                86400  IN TXT  "replicaSet=repl0"
test6.test.build.10gen.cc.                86400  IN TXT  "authSource=otherDB"
test7.test.build.10gen.cc.                86400  IN TXT  "ssl=false"
test8.test.build.10gen.cc.                86400  IN TXT  "authSource"
test10.test.build.10gen.cc.               86400  IN TXT  "socketTimeoutMS=500"
test11.test.build.10gen.cc.               86400  IN TXT  "replicaS" "et=rep" "l0"
test20.test.build.10gen.cc.               86400  IN TXT  "loadBalanced=true"
test21.test.build.10gen.cc.               86400  IN TXT  "loadBalanced=false"
test24.test.build.10gen.cc.               86400  IN TXT  "loadBalanced=true"
```

Notes:

- `test4` is omitted deliberately to test what happens with no SRV record.
- `test9` is missing because it was deleted during the development of the tests.
- The missing `test.` sub-domain in the SRV record target for `test12` is deliberate.
- `test22` is used to test a custom service name (`customname`).
- `test23` and `test24` point to port 8000 (HAProxy) and are used for load-balanced tests.

In our tests we have used `localhost.test.build.10gen.cc` as the domain, and then configured
`localhost.test.build.10gen.cc` to resolve to 127.0.0.1.

You need to adapt the records shown above to replace `test.build.10gen.cc` with your own domain name, and update the
"uri" field in the YAML or JSON files in this directory with the actual domain.

## Test Format and Use

These YAML and JSON files contain the following fields:

- `uri`: a `mongodb+srv` connection string
- `seeds`: the expected set of initial seeds discovered from the SRV record
- `numSeeds`: the expected number of initial seeds discovered from the SRV record. This is mainly used to test
  `srvMaxHosts`, since randomly selected hosts cannot be deterministically asserted.
- `hosts`: the discovered topology's list of hosts once SDAM completes a scan
- `numHosts`: the expected number of hosts discovered once SDAM completes a scan. This is mainly used to test
  `srvMaxHosts`, since randomly selected hosts cannot be deterministically asserted.
- `options`: the parsed [URI options](../../uri-options/uri-options.md) as discovered from the
  [Connection String](../../connection-string/connection-string-spec.md)'s "Connection Options" component and SRV
  resolution (e.g. TXT records, implicit `tls` default).
- `parsed_options`: additional, parsed options from other
  [Connection String](../../connection-string/connection-string-spec.md) components. This is mainly used for asserting
  `UserInfo` (as `user` and `password`) and `Auth database` (as `auth_database`).
- `error`: indicates that the parsing of the URI, or the resolving or contents of the SRV or TXT records included
  errors.
- `comment`: a comment to indicate why a test would fail.
- `ping`: if false, the test runner should not run a "ping" operation.

For each YAML file:

- Create a MongoClient initialized with the `mongodb+srv` connection string.
- Run a "ping" operation unless `ping` is false or `error` is true.

Assertions:

- If `seeds` is specified, drivers SHOULD verify that the set of hosts in the client's initial seedlist matches the list
  in `seeds`. If `numSeeds` is specified, drivers SHOULD verify that the size of that set matches `numSeeds`.

- If `hosts` is specified, drivers MUST verify that the set of ServerDescriptions in the client's TopologyDescription
  eventually matches the list in `hosts`. If `numHosts` is specified, drivers MUST verify that the size of that set
  matches `numHosts`.

- If `options` is specified, drivers MUST verify each of the values under `options` match the MongoClient's parsed value
  for that option. There may be other options parsed by the MongoClient as well, which a test does not verify.

- If `parsed_options` is specified, drivers MUST verify that each of the values under `parsed_options` match the
  MongoClient's parsed value for that option. Supported values include, but are not limited to, `user` and `password`
  (parsed from `UserInfo`) and `auth_database` (parsed from `Auth database`).

- If `error` is specified and `true`, drivers MUST verify that initializing the MongoClient throws an error. If `error`
  is not specified or is `false`, both initializing the MongoClient and running a ping operation must succeed without
  throwing any errors.

- If `ping` is not specified or `true`, drivers MUST verify that running a "ping" operation using the initialized
  MongoClient succeeds. If `ping` is `false`, drivers MUST NOT run a "ping" operation.

  > **Note:** These tests are expected to be run against MongoDB databases with and without authentication enabled. The
  > "ping" operation does not require authentication so should succeed with URIs that contain no userinfo (i.e. no
  > username and password). Tests with URIs that contain userinfo always set `ping` to `false` because some drivers will
  > fail handshake on a connection if userinfo is provided but incorrect.
