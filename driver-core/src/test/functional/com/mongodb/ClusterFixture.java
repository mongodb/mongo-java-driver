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

package com.mongodb;

import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.NettyTransportSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncClusterBinding;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncOperationContextBinding;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.binding.AsyncSessionBinding;
import com.mongodb.internal.binding.AsyncSingleConnectionBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.OperationContextBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.binding.SessionBinding;
import com.mongodb.internal.binding.SingleConnectionBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.AsynchronousSocketChannelStreamFactory;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.DefaultInetAddressResolver;
import com.mongodb.internal.connection.InternalConnectionPoolSettings;
import com.mongodb.internal.connection.InternalOperationContextFactory;
import com.mongodb.internal.connection.MongoCredentialWithCache;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.internal.connection.SocketStreamFactory;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.internal.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.internal.connection.netty.NettyStreamFactoryFactory;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;

import javax.net.ssl.SSLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.connection.ClusterConnectionMode.LOAD_BALANCED;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.STANDALONE;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getSecondaries;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * Helper class for the acceptance tests.  Used primarily by DatabaseTestCase and FunctionalSpecification.  This fixture allows Test
 * super-classes to share functionality whilst minimising duplication.
 */
public final class ClusterFixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    public static final String MONGODB_API_VERSION = "org.mongodb.test.api.version";
    public static final String MONGODB_MULTI_MONGOS_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.multi.mongos.uri";
    public static final String SERVERLESS_TEST_SYSTEM_PROPERTY_NAME = "org.mongodb.test.serverless";
    public static final String DATA_LAKE_TEST_SYSTEM_PROPERTY_NAME = "org.mongodb.test.data.lake";
    public static final String ATLAS_SEARCH_TEST_SYSTEM_PROPERTY_NAME = "org.mongodb.test.atlas.search";
    private static final String MONGODB_OCSP_SHOULD_SUCCEED = "org.mongodb.test.ocsp.tls.should.succeed";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";
    private static final int COMMAND_NOT_FOUND_ERROR_CODE = 59;
    public static final long TIMEOUT = 120L;
    public static final Duration TIMEOUT_DURATION = Duration.ofSeconds(TIMEOUT);

    public static final TimeoutSettings TIMEOUT_SETTINGS = new TimeoutSettings(30_000, 10_000, 0, null, SECONDS.toMillis(5));
    public static final TimeoutSettings TIMEOUT_SETTINGS_WITH_TIMEOUT = TIMEOUT_SETTINGS.withTimeout(TIMEOUT, SECONDS);
    public static final TimeoutSettings TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT = TIMEOUT_SETTINGS.withTimeout(0L, MILLISECONDS);
    public static final TimeoutSettings TIMEOUT_SETTINGS_WITH_MAX_TIME = TIMEOUT_SETTINGS.withMaxTimeMS(100);
    public static final TimeoutSettings TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME = TIMEOUT_SETTINGS.withMaxAwaitTimeMS(101);
    public static final TimeoutSettings TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME =
            TIMEOUT_SETTINGS.withMaxTimeAndMaxAwaitTimeMS(101, 1001);

    public static final TimeoutSettings TIMEOUT_SETTINGS_WITH_LEGACY_SETTINGS =
            TIMEOUT_SETTINGS.withMaxTimeAndMaxAwaitTimeMS(101, 1001).withMaxCommitMS(999L);
    public static final TimeoutSettings TIMEOUT_SETTINGS_WITH_MAX_COMMIT = TIMEOUT_SETTINGS.withMaxCommitMS(999L);

    public static final String LEGACY_HELLO = "isMaster";

    private static ConnectionString connectionString;
    private static Cluster cluster;
    private static Cluster asyncCluster;
    private static final Map<ReadPreference, ReadWriteBinding> BINDING_MAP = new HashMap<>();
    private static final Map<ReadPreference, AsyncReadWriteBinding> ASYNC_BINDING_MAP = new HashMap<>();

    private static ServerVersion serverVersion;
    private static BsonDocument serverParameters;

    private static NettyTransportSettings nettyTransportSettings;

    static {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    private ClusterFixture() {
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    public static boolean clusterIsType(final ClusterType clusterType) {
        return getClusterDescription(getCluster()).getType() == clusterType;
    }

    public static ClusterDescription getClusterDescription(final Cluster cluster) {
        try {
            ClusterDescription clusterDescription = cluster.getCurrentDescription();
            while (clusterDescription.getType() == UNKNOWN) {
                Thread.sleep(10);
                clusterDescription = cluster.getCurrentDescription();
            }
            return clusterDescription;
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted", e);
        }
    }

    public static ServerVersion getServerVersion() {
        if (serverVersion == null) {
            serverVersion = getVersion(new CommandReadOperation<>("admin",
                    new BsonDocument("buildInfo", new BsonInt32(1)), new BsonDocumentCodec())
                    .execute(new ClusterBinding(getCluster(), ReadPreference.nearest(), ReadConcern.DEFAULT, OPERATION_CONTEXT)));
        }
        return serverVersion;
    }

    public static final OperationContext OPERATION_CONTEXT = new OperationContext(
            IgnorableRequestContext.INSTANCE,
            new ReadConcernAwareNoOpSessionContext(ReadConcern.DEFAULT),
            new TimeoutContext(TIMEOUT_SETTINGS),
            getServerApi());

    public static final InternalOperationContextFactory OPERATION_CONTEXT_FACTORY =
            new InternalOperationContextFactory(TIMEOUT_SETTINGS, getServerApi());

    public static OperationContext createOperationContext(final TimeoutSettings timeoutSettings) {
        return new OperationContext(
                IgnorableRequestContext.INSTANCE,
                new ReadConcernAwareNoOpSessionContext(ReadConcern.DEFAULT),
                new TimeoutContext(timeoutSettings),
                getServerApi());
    }

    private static ServerVersion getVersion(final BsonDocument buildInfoResult) {
        List<BsonValue> versionArray = buildInfoResult.getArray("versionArray").subList(0, 3);

        return new ServerVersion(asList(versionArray.get(0).asInt32().getValue(),
                versionArray.get(1).asInt32().getValue(),
                versionArray.get(2).asInt32().getValue()));
    }

    public static boolean serverVersionAtLeast(final int majorVersion, final int minorVersion) {
        return getServerVersion().compareTo(new ServerVersion(asList(majorVersion, minorVersion, 0))) >= 0;
    }

    public static boolean serverVersionLessThan(final int majorVersion, final int minorVersion) {
        return getServerVersion().compareTo(new ServerVersion(asList(majorVersion, minorVersion, 0))) < 0;
    }

    public static List<Integer> getVersionList(final String versionString) {
        List<Integer> versionList = new ArrayList<>();
        for (String s : versionString.split("\\.")) {
            versionList.add(Integer.valueOf(s));
        }
        while (versionList.size() < 3) {
            versionList.add(0);
        }
        return versionList;
    }

    public static boolean hasEncryptionTestsEnabled() {
        List<String> requiredSystemProperties = asList("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AZURE_TENANT_ID", "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET", "GCP_EMAIL", "GCP_PRIVATE_KEY", "AWS_TEMP_ACCESS_KEY_ID", "AWS_TEMP_SECRET_ACCESS_KEY",
                "AWS_TEMP_SESSION_TOKEN");
        return requiredSystemProperties.stream()
                        .map(name -> getEnv(name, ""))
                        .filter(s -> !s.isEmpty())
                        .count() == requiredSystemProperties.size();
    }

    public static Document getServerStatus() {
        return new CommandReadOperation<>("admin", new BsonDocument("serverStatus", new BsonInt32(1)),
                new DocumentCodec())
                .execute(getBinding());
    }

    public static boolean supportsFsync() {
        Document serverStatus = getServerStatus();
        Document storageEngine = (Document) serverStatus.get("storageEngine");

        return storageEngine != null && !storageEngine.get("name").equals("inMemory");
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (cluster != null) {
                new DropDatabaseOperation(getDefaultDatabaseName(), WriteConcern.ACKNOWLEDGED).execute(getBinding());
                cluster.close();
            }
        }
    }

    public static String getEnv(final String name, final String defaultValue) {
        String value = getEnv(name);
        return value == null ? defaultValue : value;
    }

    @Nullable
    public static String getEnv(final String name) {
        return System.getenv(name);
    }

    public static boolean getOcspShouldSucceed() {
        return Integer.parseInt(System.getProperty(MONGODB_OCSP_SHOULD_SUCCEED)) == 1;
    }

    @Nullable
    public static synchronized ConnectionString getMultiMongosConnectionString() {
        return getConnectionStringFromSystemProperty(MONGODB_MULTI_MONGOS_URI_SYSTEM_PROPERTY_NAME);
    }

    public static boolean isServerlessTest() {
        return System.getProperty(SERVERLESS_TEST_SYSTEM_PROPERTY_NAME, "").equals("true");
    }

    public static synchronized boolean isDataLakeTest() {
        String isDataLakeSystemProperty = System.getProperty(DATA_LAKE_TEST_SYSTEM_PROPERTY_NAME);
        return "true".equals(isDataLakeSystemProperty);
    }

    public static synchronized ConnectionString getConnectionString() {
        if (connectionString != null) {
            return connectionString;
        }

        ConnectionString mongoURIProperty = getConnectionStringFromSystemProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
        if (mongoURIProperty != null) {
            return mongoURIProperty;
        }

        // Figure out what the connection string should be
        Cluster cluster = createCluster(new ConnectionString(DEFAULT_URI),
                new SocketStreamFactory(new DefaultInetAddressResolver(), SocketSettings.builder().build(), SslSettings.builder().build()));
        try {
            BsonDocument helloResult = new CommandReadOperation<>("admin",
                    new BsonDocument(LEGACY_HELLO, new BsonInt32(1)), new BsonDocumentCodec())
                    .execute(new ClusterBinding(cluster, ReadPreference.nearest(), ReadConcern.DEFAULT, OPERATION_CONTEXT));
            if (helloResult.containsKey("setName")) {
                connectionString = new ConnectionString(DEFAULT_URI + "/?replicaSet="
                        + helloResult.getString("setName").getValue());
            } else {
                connectionString = new ConnectionString(DEFAULT_URI);
                ClusterFixture.cluster = cluster;
            }

            return connectionString;
        } finally {
            if (ClusterFixture.cluster == null) {
                cluster.close();
            }
        }
    }

    public static ClusterConnectionMode getClusterConnectionMode() {
        return getCluster().getCurrentDescription().getConnectionMode();
    }

    @Nullable
    public static ServerApi getServerApi() {
         if (System.getProperty(MONGODB_API_VERSION) == null) {
             return null;
         } else {
             return ServerApi.builder().version(ServerApiVersion.findByValue(System.getProperty(MONGODB_API_VERSION))).build();
         }
    }

    public static String getConnectionStringSystemPropertyOrDefault() {
        return System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME, DEFAULT_URI);
    }

    @Nullable
    private static ConnectionString getConnectionStringFromSystemProperty(final String property) {
        String mongoURIProperty = System.getProperty(property);
        if (mongoURIProperty != null && !mongoURIProperty.isEmpty()) {
            return new ConnectionString(mongoURIProperty);
        }
        return null;
    }

    public static ReadWriteBinding getBinding() {
        return getBinding(getCluster());
    }

    public static ReadWriteBinding getBinding(final Cluster cluster) {
        return new ClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, OPERATION_CONTEXT);
    }

    public static ReadWriteBinding getBinding(final TimeoutSettings timeoutSettings) {
        return getBinding(getCluster(), ReadPreference.primary(), createNewOperationContext(timeoutSettings));
    }

    public static ReadWriteBinding getBinding(final OperationContext operationContext) {
       return getBinding(getCluster(), ReadPreference.primary(), operationContext);
    }

    public static ReadWriteBinding getBinding(final ReadPreference readPreference) {
        return getBinding(getCluster(), readPreference, OPERATION_CONTEXT);
    }

    public static OperationContext createNewOperationContext(final TimeoutSettings timeoutSettings) {
        return new OperationContext(OPERATION_CONTEXT.getId(),
                OPERATION_CONTEXT.getRequestContext(),
                OPERATION_CONTEXT.getSessionContext(),
                new TimeoutContext(timeoutSettings),
                OPERATION_CONTEXT.getServerApi());
    }

    private static ReadWriteBinding getBinding(final Cluster cluster,
            final ReadPreference readPreference,
            final OperationContext operationContext) {
        if (!BINDING_MAP.containsKey(readPreference)) {
            ReadWriteBinding binding = new SessionBinding(new ClusterBinding(cluster, readPreference, ReadConcern.DEFAULT,
                    operationContext));
            BINDING_MAP.put(readPreference, binding);
        }
        ReadWriteBinding readWriteBinding = BINDING_MAP.get(readPreference);
        return new OperationContextBinding(readWriteBinding,
                operationContext.withSessionContext(readWriteBinding.getOperationContext().getSessionContext()));
    }

    public static SingleConnectionBinding getSingleConnectionBinding() {
        return new SingleConnectionBinding(getCluster(), ReadPreference.primary(), OPERATION_CONTEXT);
    }

    public static AsyncSingleConnectionBinding getAsyncSingleConnectionBinding() {
        return getAsyncSingleConnectionBinding(getAsyncCluster());
    }

    public static AsyncSingleConnectionBinding getAsyncSingleConnectionBinding(final Cluster cluster) {
        return new AsyncSingleConnectionBinding(cluster,  ReadPreference.primary(), OPERATION_CONTEXT);
    }

    public static AsyncReadWriteBinding getAsyncBinding(final Cluster cluster) {
        return new AsyncClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, OPERATION_CONTEXT);
    }

    public static AsyncReadWriteBinding getAsyncBinding() {
        return getAsyncBinding(getAsyncCluster(), ReadPreference.primary(), OPERATION_CONTEXT);
    }

    public static AsyncReadWriteBinding getAsyncBinding(final TimeoutSettings timeoutSettings) {
        return getAsyncBinding(createNewOperationContext(timeoutSettings));
    }

    public static AsyncReadWriteBinding getAsyncBinding(final OperationContext operationContext) {
        return getAsyncBinding(getAsyncCluster(), ReadPreference.primary(), operationContext);
    }

    public static AsyncReadWriteBinding getAsyncBinding(final ReadPreference readPreference) {
        return getAsyncBinding(getAsyncCluster(), readPreference, OPERATION_CONTEXT);
    }

    public static AsyncReadWriteBinding getAsyncBinding(
            final Cluster cluster,
            final ReadPreference readPreference,
            final OperationContext operationContext) {
        if (!ASYNC_BINDING_MAP.containsKey(readPreference)) {
            AsyncReadWriteBinding binding = new AsyncSessionBinding(new AsyncClusterBinding(cluster, readPreference, ReadConcern.DEFAULT,
                    operationContext));
            ASYNC_BINDING_MAP.put(readPreference, binding);
        }
        AsyncReadWriteBinding readWriteBinding = ASYNC_BINDING_MAP.get(readPreference);
        return new AsyncOperationContextBinding(readWriteBinding,
                operationContext.withSessionContext(readWriteBinding.getOperationContext().getSessionContext()));
    }

    public static synchronized Cluster getCluster() {
        if (cluster == null) {
            cluster = createCluster(new SocketStreamFactory(new DefaultInetAddressResolver(), getSocketSettings(), getSslSettings()));
        }
        return cluster;
    }

    public static synchronized Cluster getAsyncCluster() {
        if (asyncCluster == null) {
            asyncCluster = createCluster(getAsyncStreamFactory());
        }
        return asyncCluster;
    }

    public static Cluster createCluster(final StreamFactory streamFactory) {
        return createCluster(getConnectionString(), streamFactory);
    }


    public static Cluster createCluster(final MongoCredential credential) {
        return createCluster(credential, getStreamFactory());
    }

    public static Cluster createAsyncCluster(final MongoCredential credential) {
        return createCluster(credential, getAsyncStreamFactory());
    }

    private static Cluster createCluster(final MongoCredential credential, final StreamFactory streamFactory) {
        return new DefaultClusterFactory().createCluster(ClusterSettings.builder().hosts(asList(getPrimary())).build(),
                ServerSettings.builder().build(),
                ConnectionPoolSettings.builder().maxSize(1).build(), InternalConnectionPoolSettings.builder().build(),
                TIMEOUT_SETTINGS.connectionOnly(), streamFactory, TIMEOUT_SETTINGS.connectionOnly(), streamFactory, credential,
                LoggerSettings.builder().build(), null, null, null, Collections.emptyList(), getServerApi(), null);
    }

    private static Cluster createCluster(final ConnectionString connectionString, final StreamFactory streamFactory) {
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

        return new DefaultClusterFactory().createCluster(mongoClientSettings.getClusterSettings(),
                mongoClientSettings.getServerSettings(), mongoClientSettings.getConnectionPoolSettings(),
                InternalConnectionPoolSettings.builder().build(), TimeoutSettings.create(mongoClientSettings).connectionOnly(),
                streamFactory, TimeoutSettings.createHeartbeatSettings(mongoClientSettings).connectionOnly(),
                new SocketStreamFactory(new DefaultInetAddressResolver(), SocketSettings.builder().readTimeout(5, SECONDS).build(),
                        getSslSettings(connectionString)),
                connectionString.getCredential(),
                LoggerSettings.builder().build(), null, null, null,
                connectionString.getCompressorList(), getServerApi(), null);
    }

    public static StreamFactory getStreamFactory() {
        return new SocketStreamFactory(new DefaultInetAddressResolver(), SocketSettings.builder().build(), getSslSettings());
    }

    public static StreamFactory getAsyncStreamFactory() {
        TransportSettings transportSettings = getOverriddenTransportSettings();
        if (transportSettings == null) { // use NIO2
            if (getSslSettings().isEnabled()) {
                return new TlsChannelStreamFactoryFactory(new DefaultInetAddressResolver()).create(getSocketSettings(), getSslSettings());
            } else {
                return new AsynchronousSocketChannelStreamFactory(new DefaultInetAddressResolver(), getSocketSettings(), getSslSettings());
            }
        } else {
            StreamFactoryFactory overriddenStreamFactoryFactory = NettyStreamFactoryFactory.builder()
                    .applySettings((NettyTransportSettings) transportSettings)
                    .inetAddressResolver(new DefaultInetAddressResolver())
                    .build();
            return assertNotNull(overriddenStreamFactoryFactory).create(getSocketSettings(), getSslSettings());
        }
    }

    @Nullable
    public static TransportSettings getOverriddenTransportSettings() {
        String streamType = System.getProperty("org.mongodb.test.async.type", "nio2");

        if (nettyTransportSettings == null && streamType.equals("netty")) {
            NettyTransportSettings.Builder builder = TransportSettings.nettyBuilder();
            String sslProvider = System.getProperty("org.mongodb.test.netty.ssl.provider");
            if (sslProvider != null) {
                SslContext sslContext;
                try {
                    sslContext = SslContextBuilder.forClient()
                            .sslProvider(SslProvider.valueOf(sslProvider))
                            .build();
                } catch (SSLException e) {
                    throw new MongoClientException("Unable to create Netty SslContext", e);
                }
                builder.sslContext(sslContext);
            }
            nettyTransportSettings = builder.build();
        }
        return nettyTransportSettings;
    }

    private static SocketSettings getSocketSettings() {
        return SocketSettings.builder().applyConnectionString(getConnectionString()).build();
    }

    public static SslSettings getSslSettings() {
        return getSslSettings(getConnectionString());
    }

    public static SslSettings getSslSettings(final ConnectionString connectionString) {
        return SslSettings.builder().applyConnectionString(connectionString).build();
    }

    public static ServerDescription getPrimaryServerDescription() {
        List<ServerDescription> serverDescriptions = getPrimaries(getClusterDescription(getCluster()));
        while (serverDescriptions.isEmpty()) {
            sleep(100);
            serverDescriptions = getPrimaries(getClusterDescription(getCluster()));
        }
        return serverDescriptions.get(0);
    }

    public static ServerAddress getPrimary() {
        return getPrimaryServerDescription().getAddress();
    }

    public static long getPrimaryRTT() {
        return MILLISECONDS.convert(getPrimaryServerDescription().getRoundTripTimeNanos(), NANOSECONDS);
    }

    public static ServerAddress getSecondary() {
        List<ServerDescription> serverDescriptions = getSecondaries(getClusterDescription(getCluster()));
        while (serverDescriptions.isEmpty()) {
            sleep(100);
            serverDescriptions = getSecondaries(getClusterDescription(getCluster()));
        }
        return serverDescriptions.get(0).getAddress();
    }

    public static void sleep(final int sleepMS) {
        try {
            Thread.sleep(sleepMS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    public static MongoCredential getCredential() {
        return getConnectionString().getCredential();
    }

    public static String getLoginContextName() {
        return System.getProperty("org.mongodb.test.gssapi.login.context.name", "com.sun.security.jgss.krb5.initiate");
    }

    @Nullable
    public static MongoCredentialWithCache getCredentialWithCache() {
        return getConnectionString().getCredential() == null ? null : new MongoCredentialWithCache(getConnectionString().getCredential());
    }

    public static BsonDocument getServerParameters() {
        if (serverParameters == null) {
            serverParameters = new CommandReadOperation<>("admin",
                    new BsonDocument("getParameter", new BsonString("*")), new BsonDocumentCodec())
                    .execute(getBinding());
        }
        return serverParameters;
    }

    public static boolean isDiscoverableReplicaSet() {
        return clusterIsType(REPLICA_SET) && getClusterConnectionMode() == MULTIPLE;
    }

    public static boolean isSharded() {
        return clusterIsType(SHARDED);
    }

    public static boolean isStandalone() {
        return clusterIsType(STANDALONE);
    }

    public static boolean isLoadBalanced() {
        return getClusterConnectionMode() == LOAD_BALANCED;
    }

    public static boolean isAuthenticated() {
        return getConnectionString().getCredential() != null;
    }

    public static boolean isClientSideEncryptionTest() {
        return !getEnv("AWS_ACCESS_KEY_ID", "").isEmpty();
    }

    public static boolean isAtlasSearchTest() {
        return System.getProperty(ATLAS_SEARCH_TEST_SYSTEM_PROPERTY_NAME) != null;
    }

    public static void enableMaxTimeFailPoint() {
        configureFailPoint(BsonDocument.parse("{configureFailPoint: 'maxTimeAlwaysTimeOut', mode: 'alwaysOn'}"));
    }

    public static void disableMaxTimeFailPoint() {
        disableFailPoint("maxTimeAlwaysTimeOut");
    }

    public static void enableOnPrimaryTransactionalWriteFailPoint(final BsonValue failPointData) {
        BsonDocument command = BsonDocument.parse("{ configureFailPoint: 'onPrimaryTransactionalWrite'}");

        if (failPointData.isDocument() && failPointData.asDocument().containsKey("mode")) {
            for (Map.Entry<String, BsonValue> keyValue : failPointData.asDocument().entrySet()) {
                command.append(keyValue.getKey(), keyValue.getValue());
            }
        } else {
            command.append("mode", failPointData);
        }
        configureFailPoint(command);
    }

    public static void disableOnPrimaryTransactionalWriteFailPoint() {
        disableFailPoint("onPrimaryTransactionalWrite");
    }

    public static void configureFailPoint(final BsonDocument failPointDocument) {
        assumeThat(isSharded(), is(false));
        boolean failsPointsSupported = true;
        if (!isSharded()) {
            try {
                new CommandReadOperation<>("admin", failPointDocument, new BsonDocumentCodec())
                        .execute(getBinding());
            } catch (MongoCommandException e) {
                if (e.getErrorCode() == COMMAND_NOT_FOUND_ERROR_CODE) {
                    failsPointsSupported = false;
                }
            }
            assumeTrue("configureFailPoint is not enabled", failsPointsSupported);
        }
    }

    public static void disableFailPoint(final String failPoint) {
        if (!isSharded()) {
            BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString(failPoint))
                    .append("mode", new BsonString("off"));
            try {
                new CommandReadOperation<>("admin", failPointDocument, new BsonDocumentCodec())
                        .execute(getBinding());
            } catch (MongoCommandException e) {
                // ignore
            }
        }
    }

    @SuppressWarnings("overloads")
    public static <T> T executeSync(final WriteOperation<T> op) {
        return executeSync(op, getBinding());
    }

    @SuppressWarnings("overloads")
    public static <T> T executeSync(final WriteOperation<T> op, final ReadWriteBinding binding) {
        return op.execute(binding);
    }

    @SuppressWarnings("overloads")
    public static <T> T executeSync(final ReadOperation<T> op) {
        return executeSync(op, getBinding());
    }

    @SuppressWarnings("overloads")
    public static <T> T executeSync(final ReadOperation<T> op, final ReadWriteBinding binding) {
        return op.execute(binding);
    }

    @SuppressWarnings("overloads")
    public static <T> T executeAsync(final AsyncWriteOperation<T> op) throws Throwable {
        return executeAsync(op, getAsyncBinding());
    }

    @SuppressWarnings("overloads")
    public static <T> T executeAsync(final AsyncWriteOperation<T> op, final AsyncWriteBinding binding) throws Throwable {
        FutureResultCallback<T> futureResultCallback = new FutureResultCallback<>();
        op.executeAsync(binding, futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    @SuppressWarnings("overloads")
    public static <T> T executeAsync(final AsyncReadOperation<T> op) throws Throwable {
        return executeAsync(op, getAsyncBinding());
    }

    @SuppressWarnings("overloads")
    public static <T> T executeAsync(final AsyncReadOperation<T> op, final AsyncReadBinding binding) throws Throwable {
        FutureResultCallback<T> futureResultCallback = new FutureResultCallback<>();
        op.executeAsync(binding, futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static <T> void loopCursor(final List<AsyncBatchCursor<T>> batchCursors, final Block<T> block) throws Throwable {
        List<FutureResultCallback<Void>> futures = new ArrayList<>();
        for (AsyncBatchCursor<T> batchCursor : batchCursors) {
            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
            futures.add(futureResultCallback);
            loopCursor(batchCursor, block, futureResultCallback);
        }
        for (int i = 0; i < batchCursors.size(); i++) {
            futures.get(i).get(TIMEOUT, SECONDS);
        }
    }

    public static <T> void loopCursor(final AsyncReadOperation<AsyncBatchCursor<T>> op, final Block<T> block) throws Throwable {
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
        loopCursor(executeAsync(op), block, futureResultCallback);
        futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static <T> void loopCursor(final AsyncBatchCursor<T> batchCursor, final Block<T> block,
                                      final SingleResultCallback<Void> callback) {
        if (batchCursor.isClosed()) {
            callback.onResult(null, null);
            return;
        }
        batchCursor.next((results, t) -> {
            if (t != null || results == null) {
                batchCursor.close();
                callback.onResult(null, t);
            } else {
                try {
                    for (T result : results) {
                        block.apply(result);
                    }
                    loopCursor(batchCursor, block, callback);
                } catch (Throwable tr) {
                    batchCursor.close();
                    callback.onResult(null, tr);
                }
            }
        });
    }

    public static <T> List<T> collectCursorResults(final AsyncBatchCursor<T> batchCursor) throws Throwable {
        List<T> results = new ArrayList<>();
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
        loopCursor(batchCursor, t -> results.add(t), futureResultCallback);
        futureResultCallback.get(TIMEOUT, SECONDS);
        return results;
    }

    public static <T> List<T> collectCursorResults(final BatchCursor<T> batchCursor) {
        List<T> results = new ArrayList<>();
        while (batchCursor.hasNext()) {
            results.addAll(batchCursor.next());
        }
        return results;
    }

    public static AsyncConnectionSource getWriteConnectionSource(final AsyncReadWriteBinding binding) throws Throwable {
        FutureResultCallback<AsyncConnectionSource> futureResultCallback = new FutureResultCallback<>();
        binding.getWriteConnectionSource(futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static AsyncConnectionSource getReadConnectionSource(final AsyncReadWriteBinding binding) throws Throwable {
        FutureResultCallback<AsyncConnectionSource> futureResultCallback = new FutureResultCallback<>();
        binding.getReadConnectionSource(futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static AsyncConnection getConnection(final AsyncConnectionSource source) throws Throwable {
        FutureResultCallback<AsyncConnection> futureResultCallback = new FutureResultCallback<>();
        source.getConnection(futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static synchronized void checkReferenceCountReachesTarget(final ReferenceCounted referenceCounted, final int target) {
        int count = getReferenceCountAfterTimeout(referenceCounted, target);
        if (count != target) {
            throw new MongoTimeoutException(
                    format("Timed out waiting for reference count to drop to %d.  Now at %d for %s", target, count,
                            referenceCounted));
        }
    }

    public static int getReferenceCountAfterTimeout(final ReferenceCounted referenceCounted, final int target) {
        long startTime = System.currentTimeMillis();
        int count = referenceCounted.getCount();
        while (count > target) {
            try {
                if (System.currentTimeMillis() > startTime + TIMEOUT_DURATION.toMillis()) {
                    return count;
                }
                Thread.sleep(10);
                count = referenceCounted.getCount();
            } catch (InterruptedException e) {
                throw interruptAndCreateMongoInterruptedException("Interrupted", e);
            }
        }
        return count;
    }

    public static ClusterSettings.Builder setDirectConnection(final ClusterSettings.Builder builder) {
        return builder.mode(ClusterConnectionMode.SINGLE).hosts(singletonList(getPrimary()));
    }

    public static int applyTimeoutMultiplierForServerless(final int timeoutMs) {
        if (ClusterFixture.isServerlessTest()) {
            return timeoutMs * 2;
        }
        return timeoutMs;
    }
}
