package org.mongodb.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.file.util.BytesCopier;

public class MongoFileCursorTest extends DatabaseTestCase {

    private static final String BUCKET = "cursor";

    private MongoFileStore store;

    // initializer
    @Before
    public void setup() throws IOException {

        super.setUp();

        // clean up
        database.getCollection(BUCKET + ".files").tools().drop();
        database.getCollection(BUCKET + ".chunks").tools().drop();

        // populate for test
        MongoFileStoreConfig config = MongoFileStoreConfig.builder().bucket(BUCKET).enableCompression(false).build();
        store = new MongoFileStore(database, config);

        createFile(store, "/foo/bar1.txt", "text/plain");
        createFile(store, "/foo/bar4.txt", "text/plain");
        createFile(store, "/baz/bar3.txt", "text/plain");
        createFile(store, "/foo/bar1.txt", "text/plain");
    }

    @Test
    public void testSimpleList() throws IOException {

        MongoFileCursor fileList = store.find(new Document());
        int count = 0;
        for (MongoFile mongoFile : fileList) {
            ++count;
            assertNotNull(mongoFile.getURL());
        }
        assertEquals(4, count);
    }

    @Test
    public void testFilterFileNameList() throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream(32 * 1024);

        MongoFileCursor cursor = store.find("/foo/bar1.txt");
        int count = 0;
        for (MongoFile mongoFile : cursor) {
            ++count;
            assertNotNull(mongoFile.getURL());
            assertEquals("/foo/bar1.txt", mongoFile.getFilename());
            new BytesCopier(mongoFile.getInputStream(), out).transfer(true); // append more than one file together
        }
        assertEquals(2, count);
        assertEquals(LoremIpsum.getString().length() * 2, out.toString().length());
    }

    @Test
    public void testSortedList() throws IOException {

        MongoFileCursor fileList = store.find(new Document(), new Document("filename", 1));

        assertTrue(fileList.hasNext());
        assertEquals("/baz/bar3.txt", fileList.next().getFilename());

        assertTrue(fileList.hasNext());
        assertEquals("/foo/bar1.txt", fileList.next().getFilename());

        assertTrue(fileList.hasNext());
        assertEquals("/foo/bar1.txt", fileList.next().getFilename());

        assertTrue(fileList.hasNext());
        assertEquals("/foo/bar4.txt", fileList.next().getFilename());

        assertFalse(fileList.hasNext());
    }

    @Test
    public void testSortedFilteredList() throws IOException {

        MongoFileCursor fileList = store.find(new Document("filename", "/foo/bar1.txt"), new Document("filename", 1));

        assertTrue(fileList.hasNext());
        assertEquals("/foo/bar1.txt", fileList.next().getFilename());

        assertTrue(fileList.hasNext());
        assertEquals("/foo/bar1.txt", fileList.next().getFilename());

        assertFalse(fileList.hasNext());
    }

    @Test
    public void testFindList() throws IOException {

        List<MongoFile> fileList = store.find("/foo/bar1.txt").toList();

        assertEquals(2, fileList.size());
        assertEquals("/foo/bar1.txt", fileList.get(0).getFilename());
        assertEquals("/foo/bar1.txt", fileList.get(1).getFilename());
    }

    //
    // internal
    // //////////////////
    private void createFile(final MongoFileStore store, final String filename, final String mediaType) throws IOException {

        MongoFileWriter writer = store.createNew(filename, mediaType);
        writer.write(new ByteArrayInputStream(LoremIpsum.getBytes()));
    }
}
