package org.mongodb.file;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.file.writing.CountingOutputStream;

public class CountingOutputStreamTest extends DatabaseTestCase {

    private MongoFileStore store;

    @Before
    public void setup() {
        super.setUp();

        MongoFileStoreConfig config = MongoFileStoreConfig.builder().bucket("gzip").build();
        store = new MongoFileStore(database, config);
    }

    @Test
    public void test() throws IOException {

        // MongoFile mock = Mockito.mock(MongoFile.class);
        MongoFile mongoFile = new MongoFile(store, new Document());

        ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024);

        CountingOutputStream stream = new CountingOutputStream(MongoFileConstants.chunkSize, mongoFile, out);
        try {
            byte[] bytes = LoremIpsum.getBytes();
            stream.write(bytes);
            stream.write(123);

            assertEquals(bytes.length + 1, stream.getCount());
        } finally {
            stream.close();
        }

    }
}
