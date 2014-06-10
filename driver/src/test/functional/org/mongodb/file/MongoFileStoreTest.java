package org.mongodb.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.WriteConcern;
import org.mongodb.file.util.BytesCopier;
import org.mongodb.file.util.ChunkSize;

public class MongoFileStoreTest extends DatabaseTestCase {

    @Test
    public void testBasicUncompressedRoundTrip() throws IOException {

        doRoundTrip("mongofs", "loremIpsum.txt", MongoFileStoreConfig.DEFAULT_CHUNKSIZE, false);
    }

    @Test
    public void testBasicCompressedRoundTrip() throws IOException {

        doRoundTrip("mongofs", "loremIpsum.txt", MongoFileStoreConfig.DEFAULT_CHUNKSIZE, true);
    }

    @Test
    public void testLotsOfChunksUncompressedRoundTrip() throws IOException {

        doRoundTrip("mongofs", "loremIpsum.txt", ChunkSize.tiny_4K, false);
    }

    @Test
    public void testLotsOfChunksCompressedRoundTrip() throws IOException {

        doRoundTrip("mongofs", "loremIpsum.txt", ChunkSize.tiny_4K, true);
    }

    //
    // internal
    // /////////////////

    private void doRoundTrip(final String bucket, final String filename, final ChunkSize chunkSize, final boolean compress)
            throws IOException {

        MongoFileStoreConfig config = MongoFileStoreConfig.builder()//
                .bucket(bucket).chunkSize(chunkSize).writeConcern(WriteConcern.JOURNALED).build();

        MongoFileStore store = new MongoFileStore(database, config);

        MongoFileWriter writer = store.createNew(filename, "text/plain", null, compress);
        ByteArrayInputStream in = new ByteArrayInputStream(LoremIpsum.getBytes());
        OutputStream out = writer.getOutputStream();
        try {
            new BytesCopier(in, out).transfer(true);
        } finally {
            out.close();
        }

        // verify it exists
        MongoFile file = writer.getMongoFile();
        assertTrue(store.exists(file.getURL()));

        // read a file
        MongoFile mongoFile = store.findOne(file.getURL());
        assertEquals(compress, mongoFile.getURL().isStoredCompressed());
        assertEquals(LoremIpsum.LOREM_IPSUM.length(), mongoFile.getLength());
        if (compress) {
            assertNotNull(mongoFile.get(MongoFileConstants.compressedLength)); // verify compression
            assertNotNull(mongoFile.get(MongoFileConstants.compressionFormat)); // verify compression
            assertNotNull(mongoFile.get(MongoFileConstants.compressionRatio)); // verify compression
        } else {
            assertNull(mongoFile.get(MongoFileConstants.compressedLength)); // verify no compression
            assertNull(mongoFile.get(MongoFileConstants.compressionFormat)); // verify no compression
            assertNull(mongoFile.get(MongoFileConstants.compressionRatio)); // verify no compression
        }

        assertEquals(LoremIpsum.getString(), mongoFile.readInto(new ByteArrayOutputStream(32 * 1024), true).toString());

        // remove a file
        // store.remove(mongoFile, true); // flag delete

        // verify it does not exist
        // assertFalse(store.exists(mongoFile.getURL()));
    }
}
