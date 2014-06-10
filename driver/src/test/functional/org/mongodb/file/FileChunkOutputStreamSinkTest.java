package org.mongodb.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoView;
import org.mongodb.file.writing.ChunksStatisticsAdapter;
import org.mongodb.file.writing.FileChunksOutputStreamSink;

public class FileChunkOutputStreamSinkTest extends DatabaseTestCase {

    private MongoFileStore store;

    @Before
    public void setUp() {
        super.setUp();
        store = new MongoFileStore(database, MongoFileStoreConfig.builder().bucket("buffer").enableCompression(false).build());
    }

    @Test
    public void testFullBufferWrite() throws IOException {

        MongoFileWriter writer = store.createNew("foo.txt", "text/plain");

        MongoFile mongoFile = writer.getMongoFile();
        ChunksStatisticsAdapter adapter = new MongoFileWriterAdapter(mongoFile);

        FileChunksOutputStreamSink sink = new FileChunksOutputStreamSink(//
                store.getChunksCollection(), mongoFile.getId(), adapter, null);
        try {
            byte[] array = "This is a test".getBytes();
            sink.write(array, 0, array.length);
        } finally {
            sink.close();
        }

        // assert
        MongoView<Document> mongoView = store.getChunksCollection().find(new Document("files_id", mongoFile.getId()));

        assertNotNull(mongoView);
        assertEquals(1, mongoView.count());

        Document document = mongoView.get().next();
        assertNotNull(document);
        assertEquals(mongoFile.getId(), document.get("files_id"));

        assertNotNull(document.get("data"));
        byte[] bytes = (byte[]) document.get("data");
        assertEquals(14, bytes.length);
        assertEquals("This is a test", new String(bytes, "UTF-8"));

    }

    @Test
    public void testPartialBufferWrite() throws IOException {

        MongoFileWriter writer = store.createNew("foo.txt", "text/plain");

        MongoFile mongoFile = writer.getMongoFile();
        ChunksStatisticsAdapter adapter = new MongoFileWriterAdapter(mongoFile);

        FileChunksOutputStreamSink sink = new FileChunksOutputStreamSink(//
                store.getChunksCollection(), mongoFile.getId(), adapter, null);
        try {
            byte[] array = "This is a test".getBytes();
            sink.write(array, 10, 4);
        } finally {
            sink.close();
        }

        // assert
        MongoView<Document> mongoView = store.getChunksCollection().find(new Document("files_id", mongoFile.getId()));

        assertNotNull(mongoView);
        assertEquals(1, mongoView.count());

        Document document = mongoView.get().next();
        assertNotNull(document);
        assertEquals(mongoFile.getId(), document.get("files_id"));

        assertNotNull(document.get("data"));
        byte[] bytes = (byte[]) document.get("data");
        assertEquals(4, bytes.length);

        assertEquals("test", new String(bytes, "UTF-8"));
    }
}
