package org.mongodb.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.MongoException;
import org.mongodb.file.util.BytesCopier;

public class StorageTest extends DatabaseTestCase {

    @Test
    // @Ignore
    public void testMongoFS() throws IOException {

        String bucket = "mongofs";
        MongoFileStore store = new MongoFileStore(database, MongoFileStoreConfig.builder().bucket(bucket).build());
        MongoFileWriter writer = store.createNew("mongoFS.txt", "text/plain");
        MongoFile file = writer.getMongoFile();

        file.put("aliases", Arrays.asList("one", "two", "three"));
        file.setInMetaData("key", "value");

        writer.write(new ByteArrayInputStream(LoremIpsum.getBytes()));

        // System.out.println("MongoFS (0.x)");
        // System.out.println("==============================");
        // System.out.println(String.format("url= %s", file.getURL().toString()));
        // System.out.println("==============================");
        // System.out.println(JSONHelper.prettyPrint(file.toString()));
        // System.out.println("======");
        // dumpChunks(bucket, file.getId(), System.out);
        // System.out.println("==============================");
        // System.out.println();

        // md5 validation
        try {
            file.validate();
        } catch (MongoException e) {
            fail(e.getMessage());
        }

        assertEquals(store.getChunkSize(), file.getChunkSize());
        assertEquals(1, file.getChunkCount());
        assertNotNull(file.getUploadDate());

        // the id is always a generated UUID, thus test for everything else to be correct
        assertTrue(file.getURL().toString().startsWith("mongofile:gz:mongoFS.txt?"));
        assertTrue(file.getURL().toString().endsWith("#text/plain"));

        MongoFile findOne = store.findOne(file.getId());
        assertNotNull(findOne);

        ByteArrayOutputStream out = new ByteArrayOutputStream(32 * 1024);
        new BytesCopier(findOne.getInputStream(), out).transfer(true);
        assertEquals(LoremIpsum.getString(), out.toString());

    }
}
