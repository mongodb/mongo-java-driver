package org.mongodb.file;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.file.writing.MongoGZipOutputStream;

public class MongoGZipOutputStreamTest extends DatabaseTestCase {

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
        mongoFile.put(MongoFileConstants.length, 100L);
        mongoFile.put(MongoFileConstants.compressedLength, 50L);

        OutputStream stream = new MongoGZipOutputStream(mongoFile, new ByteArrayOutputStream(1024 * 1024));
        try {
            byte[] bytes = LoremIpsum.getBytes();
            stream.write(bytes);
            stream.write(123);

        } finally {
            stream.close();
        }

    }
}
