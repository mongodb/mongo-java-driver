package com.mongodb.client.unified;

import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static org.junit.Assume.assumeFalse;

public class ConnectionPoolLoggingTest extends UnifiedSyncTest {


    public ConnectionPoolLoggingTest(@SuppressWarnings("unused") final String fileDescription,
            @SuppressWarnings("unused") final String testDescription,
            final String schemaVersion,
            @Nullable final BsonArray runOnRequirements, final BsonArray entities, final BsonArray initialData,
            final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
        assumeFalse(isServerlessTest());
        // The driver has a hack where getLastError command is executed as part of the handshake in order to get a connectionId
        // even when the hello command response doesn't contain it.
        assumeFalse(fileDescription.equals("pre-42-server-connection-id"));
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/connection-monitoring-and-pooling");
    }
}
