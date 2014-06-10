package com.mongodb.gridfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Fixture;
import org.mongodb.MongoClient;
import org.mongodb.MongoDatabase;
import org.mongodb.file.LoremIpsum;
import org.mongodb.file.MongoFile;
import org.mongodb.file.MongoFileConstants;
import org.mongodb.file.MongoFileStore;
import org.mongodb.file.MongoFileStoreConfig;
import org.mongodb.file.util.BytesCopier;
import org.mongodb.file.util.ChunkSize;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DatabaseTestCase;
import com.mongodb.MongoException;

public class MongoFSOnTopOfGridFSTest extends DatabaseTestCase {

    private static final String bucket = "original";

    private MongoDatabase newDatabase;

    @Before
    public void setUp() {
        super.setUp();

        // from driver-compat code base
        String name = database.getName();

        // create in driver code base pointing to the same
        MongoClient mongoClient = Fixture.getMongoClient();
        newDatabase = mongoClient.getDatabase(name);
    }

    @Test
    public void doTheTest() throws IOException {

        ObjectId id = createGridFSFile();
        verifyReadFromMongoFS(id);

    }

    public ObjectId createGridFSFile() throws IOException {

        ObjectId id = new ObjectId();

        com.mongodb.gridfs.GridFS gridFS = new com.mongodb.gridfs.GridFS(database, bucket);
        com.mongodb.gridfs.GridFSInputFile file = gridFS.createFile("originalGridFS.txt");
        file.setChunkSize(ChunkSize.tiny_4K.getChunkSize());
        file.setId(id);
        file.put("aliases", Arrays.asList("one", "two", "three"));
        file.put(MongoFileConstants.contentType.toString(), "text/plain");
        file.setMetaData(new BasicDBObject("key", "value"));

        OutputStream stream = file.getOutputStream();
        try {
            stream.write(LoremIpsum.getBytes());
        } finally {
            stream.close();
        }

        // md5 validation
        try {
            file.validate();
        } catch (MongoException e) {
            fail(e.getMessage());
        }

        assertEquals(12, file.numChunks());

        ByteArrayOutputStream out = new ByteArrayOutputStream(32 * 1024);

        for (int i = 0; i < file.numChunks(); ++i) {
            DBObject findOne = gridFS.getChunksCollection().find(new BasicDBObject("files_id", id).append("n", i)).next();
            assertNotNull(findOne);
            new BytesCopier(new ByteArrayInputStream((byte[]) findOne.get("data")), out).transfer(true);
        }

        assertEquals(LoremIpsum.getString(), out.toString());
        return id;
    }

    public void verifyReadFromMongoFS(final ObjectId id) throws IOException {

        MongoFileStore store = new MongoFileStore(newDatabase, MongoFileStoreConfig.builder().bucket(bucket).build());
        MongoFile file = store.findOne(id);
        // file = store.find(id.toString()).next();
        // file = store.find(new Document("_id", id.toString())).next();

        assertEquals(12, file.getChunkCount());

        OutputStream out = file.readInto(new ByteArrayOutputStream(32 * 1024), true);
        assertEquals(LoremIpsum.getString(), out.toString());

        // System.out.println("Passed - MongoFS");
    }
}
