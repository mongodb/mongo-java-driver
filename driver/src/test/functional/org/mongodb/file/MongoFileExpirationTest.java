package org.mongodb.file;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.WriteConcern;
import org.mongodb.file.util.TimeMachine;

public class MongoFileExpirationTest extends DatabaseTestCase {

    private MongoFileStore store;

    @Before
    public void setUp() {

        super.setUp(); // database connection

        MongoFileStoreConfig config = new MongoFileStoreConfig("expire");
        config.setWriteConcern(WriteConcern.JOURNALED);
        store = new MongoFileStore(database, config);
    }

    @Test
    public void test() throws IOException {
        long now = System.currentTimeMillis();

        LoremIpsum.createTempFile(store, "/foo/bar1.txt", "text/plain", TimeMachine.now().backward(2).days().inTime());
        LoremIpsum.createTempFile(store, "/foo/bar1.txt", "text/plain", TimeMachine.from(now).forward(5).seconds().inTime());

        MongoFileCursor cursor = store.query().find("/foo/bar1.txt");
        assertTrue(Math.abs(now - (2 * 24 * 60 * 60 * 1000) - cursor.next().getExpiresAt().getTime()) <= 1);
        assertTrue(Math.abs(now + (5 * 1000) - cursor.next().getExpiresAt().getTime()) <= 1);

    }

}
