package org.mongodb.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

public class MongoFileConstantsTest {

    @Test
    public void testCoreGridFSList() {

        Set<String> coreFields = MongoFileConstants.getFields(false);
        assertNotNull(coreFields);
        assertEquals(9, coreFields.size());
        assertTrue(coreFields.contains(MongoFileConstants._id.name()));
        assertTrue(coreFields.contains(MongoFileConstants.filename.name()));
        assertTrue(coreFields.contains(MongoFileConstants.md5.name()));
        assertTrue(coreFields.contains(MongoFileConstants.chunkSize.name()));
        assertTrue(coreFields.contains(MongoFileConstants.contentType.name()));
        assertTrue(coreFields.contains(MongoFileConstants.aliases.name()));
        assertTrue(coreFields.contains(MongoFileConstants.length.name()));
        assertTrue(coreFields.contains(MongoFileConstants.uploadDate.name()));
        assertTrue(coreFields.contains(MongoFileConstants.metadata.name()));
    }

    @Test
    public void testCoreMongoFSList() {

        Set<String> coreFields = MongoFileConstants.getFields(true);
        assertNotNull(coreFields);
        assertEquals(15, coreFields.size());
        assertTrue(coreFields.contains(MongoFileConstants._id.name()));
        assertTrue(coreFields.contains(MongoFileConstants.filename.name()));
        assertTrue(coreFields.contains(MongoFileConstants.md5.name()));
        assertTrue(coreFields.contains(MongoFileConstants.chunkSize.name()));
        assertTrue(coreFields.contains(MongoFileConstants.contentType.name()));
        assertTrue(coreFields.contains(MongoFileConstants.aliases.name()));
        assertTrue(coreFields.contains(MongoFileConstants.length.name()));
        assertTrue(coreFields.contains(MongoFileConstants.uploadDate.name()));
        assertTrue(coreFields.contains(MongoFileConstants.metadata.name()));

        assertTrue(coreFields.contains(MongoFileConstants.chunkCount.name()));
        assertTrue(coreFields.contains(MongoFileConstants.compressionRatio.name()));
        assertTrue(coreFields.contains(MongoFileConstants.compressedLength.name()));
        assertTrue(coreFields.contains(MongoFileConstants.expireAt.name()));
        assertTrue(coreFields.contains(MongoFileConstants.deleted.name()));

    }
}
