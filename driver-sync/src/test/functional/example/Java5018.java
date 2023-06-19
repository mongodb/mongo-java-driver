/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.SslSettings;

import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.mongodb.assertions.Assertions.assertTrue;

/**
 * Clone <a href="https://github.com/10gen/atlasproxy">atlasproxy</a> to
 * {@code /Users/valentin.kovalenko/Documents/projects/atlasproxy/main/} and checkout the commit d63a315fa0b9d81d714855d77961dbf77c203c08.
 * Make sure you have read access to <a href="https://github.com/10gen/mongonet">mongonet</a>.
 * If you do not, then request access to {@code 10gen/mongonet} via <a href="https://mana.corp.mongodbgov.com/">MANA</a>.
 * Then do the following from Bash to build and start MongoDB and proxies that support gRPC:
 *
 * <pre>{@code
 *  $ export MONGO_DIR=/usr/local/bin/
 *  $ export RSNAME=""
 *  $ export SERVERLESS_MODE=true
 *  $ export SERVERLESS_METRICS_MODE=true
 *  $ export GRPC_MODE=true
 *  $
 *  $ # see https://wiki.corp.mongodb.com/display/MMS/MongoDB+Agent+Resources#MongoDBAgentResources-Developing
 *  $ export GOPRIVATE=github.com/10gen/cloud-agent-common,github.com/10gen/cloud-auth-common,github.com/10gen/bsonio,github.com/10gen/bsonutil,github.com/10gen/mongoast,github.com/10gen/mongonet
 *  $ git config --global url."ssh://git@github.com/10gen/cloud-agent-common".insteadOf "https://github.com/10gen/cloud-agent-common"
 *  $ git config --global url."ssh://git@github.com/10gen/cloud-auth-common".insteadOf "https://github.com/10gen/cloud-auth-common"
 *  $ git config --global url."ssh://git@github.com/10gen/mongoast".insteadOf "https://github.com/10gen/mongoast"
 *  $ git config --global url."ssh://git@github.com/10gen/bsonio".insteadOf "https://github.com/10gen/bsonio"
 *  $ git config --global url."ssh://git@github.com/10gen/bsonutil".insteadOf "https://github.com/10gen/bsonutil"
 *  $ git config --global url."ssh://git@github.com/10gen/mongonet".insteadOf "https://github.com/10gen/mongonet"
 *  $
 *  $ cd /Users/valentin.kovalenko/Documents/projects/atlasproxy/main/
 *  $ # works with go 1.20.5
 *  $ ./start_test_proxies_and_mtms.sh
 * }</pre>
 *
 * If successful, three {@code mongod} processes and three proxies will be started:
 *
 * <pre>{@code
 *  atlasproxy -mongoURI 'mongodb://u:p@host1.local.10gen.cc:27000,host2.local.10gen.cc:27010,host3.local.10gen.cc:27020/?ssl=true' -localMongoURI 'mongodb://u:p@host1.local.10gen.cc:27000/?ssl=true' -bindPort 9900 -grpcBindPort 9901 -metricsPort 8100 -configPath configReplicaSet.json -sslPEMKeyFile star.local.10gen.cc.pem -sslCAFile ca.pem -logPath=proxylogs/proxy9900.log -v -rssThresholdPct 10 -proxyHostnameForTests donorProxy9900 -remoteConfigPathForTests=configReplicaSetRecipient.json -enableTestCommands -serverlessMode=true -serverlessMetricsMode=true -testing=true
 *  atlasproxy -mongoURI 'mongodb://u:p@host1.local.10gen.cc:27000,host2.local.10gen.cc:27010,host3.local.10gen.cc:27020/?ssl=true' -localMongoURI 'mongodb://u:p@host1.local.10gen.cc:27010/?ssl=true' -bindPort 9910 -grpcBindPort 9911 -metricsPort 8110 -configPath configReplicaSet.json -sslPEMKeyFile star.local.10gen.cc.pem -sslCAFile ca.pem -logPath=proxylogs/proxy9910.log -v -rssThresholdPct 10 -proxyHostnameForTests donorProxy9910 -remoteConfigPathForTests=configReplicaSetRecipient.json -enableTestCommands -disableProfile -serverlessMode=true -serverlessMetricsMode=true -testing=true
 *  atlasproxy -mongoURI 'mongodb://u:p@host1.local.10gen.cc:27000,host2.local.10gen.cc:27010,host3.local.10gen.cc:27020/?ssl=true' -localMongoURI 'mongodb://u:p@host1.local.10gen.cc:27020/?ssl=true' -bindPort 9920 -grpcBindPort 9921 -metricsPort 8120 -configPath configReplicaSet.json -sslPEMKeyFile star.local.10gen.cc.pem -sslCAFile ca.pem -logPath=proxylogs/proxy9920.log -v -rssThresholdPct 10 -proxyHostnameForTests donorProxy9920 -remoteConfigPathForTests=configReplicaSetRecipient.json -enableTestCommands -disableProfile -serverlessMode=true -serverlessMetricsMode=true -testing=true
 * }</pre>
 *
 * Connect to the fist one, which presents itself as {@code mongos} ({@link ServerType#SHARD_ROUTER}), via {@code mongosh}:
 *
 * <pre>{@code
 *  $ mongosh "mongodb://user:pencil@host9.local.10gen.cc:9900/?tls=true" --tlsCAFile=/Users/valentin.kovalenko/Documents/projects/atlasproxy/main/ca.pem
 * }</pre>
 *
 * Create a truststore that will be used by {@link MongoClient} to authenticate the server
 * (note that {@link SslSettings.Builder#invalidHostNameAllowed(boolean)} cannot be used to disable the authentication, it may only
 * relax it when it comes to verifying the server hostname against the subject in the certificate presented by the server):
 *
 * <pre>{@code
 *  $ cd /Users/valentin.kovalenko/Documents/projects/atlasproxy/main/
 *  $ cp /Users/valentin.kovalenko/.sdkman/candidates/java/current/lib/security/cacerts ./mongo-truststore
 *  $ keytool --importcert -trustcacerts -file ./ca.pem -keystore mongo-truststore -storepass changeit -storetype JKS -noprompt
 * }</pre>
 *
 * Connect to the first one via {@link Java5018} by running it with the following {@code java} CLI arguments:
 *
 * <pre>{@code
 *  -Djavax.net.trustStoreType=jks -Djavax.net.ssl.trustStore=/Users/valentin.kovalenko/Documents/projects/atlasproxy/main/mongo-truststore -Djavax.net.ssl.trustStorePassword=changeit
 * }</pre>
 *
 * Note that the port 9900 is for mongorpc, while 9901 is for gRPC.
 */
final class Java5018 {
    public static void main(final String... args) {
        try (MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(
//                        new ConnectionString("mongodb://user:pencil@host9.local.10gen.cc:9900/?tls=true")
                        new ConnectionString("mongodb://user:pencil@host9.local.10gen.cc:9901/?gRPC=true")
                ).applyToClusterSettings(builder -> builder
                        .serverSelectionTimeout(1, TimeUnit.SECONDS)
                ).applyToSocketSettings(builder -> builder
                        .connectTimeout(1, TimeUnit.SECONDS)
                ).build())) {
            assertTrue(StreamSupport.stream(client.listDatabaseNames().spliterator(), false)
                    .anyMatch(dbName -> dbName.equals("admin")));
        }
    }

    private Java5018() {
    }
}
