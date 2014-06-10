package org.mongodb.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.file.url.MongoFileUrl;

public class MongoFileTest {

    public static final String PDF = "application/pdf";
    public static final String ZIP = "application/zip";

    @Test
    public void testGZipFactoriesItemized() throws IOException {

        ObjectId id = new ObjectId();
        MongoFileUrl url = MongoFileUrl.construct(id, "fileName.pdf", PDF, true);
        assertNotNull(url);
        assertEquals(String.format("mongofile:gz:fileName.pdf?%s#application/pdf", id.toString()), url.getUrl().toString());

        assertEquals(id, url.getMongoFileId());
        assertEquals("fileName.pdf", url.getFilePath());
        assertEquals("fileName.pdf", url.getFileName());
        assertEquals("pdf", url.getExtension());
        assertTrue(url.isStoredCompressed());
        assertFalse(url.isDataCompressed());
        assertEquals(PDF, url.getMediaType());
    }

    @Test
    public void testFactoriesItemized() throws IOException {

        ObjectId id = new ObjectId();
        MongoFileUrl url = MongoFileUrl.construct(id, "fileName.zip", ZIP, true);
        assertNotNull(url);
        assertEquals(String.format("mongofile:fileName.zip?%s#application/zip", id.toString()), url.getUrl().toString());

        assertEquals(id, url.getMongoFileId());
        assertEquals("fileName.zip", url.getFilePath());
        assertEquals("fileName.zip", url.getFileName());
        assertEquals("zip", url.getExtension());
        assertFalse(url.isStoredCompressed());
        assertTrue(url.isDataCompressed());
        assertEquals(ZIP, url.getMediaType());
    }

    @Test
    public void testFactoriesFromSpecCrosswired() throws IOException {

        // this test to to test the ability to changes what MediaTypes are compressed
        // over time without problems for existing files already stored in the database

        // This file is not-compressed but compressable, yet is was not compressed
        MongoFileUrl url = MongoFileUrl
                .construct("mongofile:/home/oildex/x0064660/invoice/report/activeusers_19.PDF?52fb1e7b36707d6d13ebfda9#application/pdf");
        assertNotNull(url);

        assertEquals(new ObjectId("52fb1e7b36707d6d13ebfda9"), url.getMongoFileId());
        assertEquals("/home/oildex/x0064660/invoice/report/activeusers_19.PDF", url.getFilePath());
        assertEquals("activeusers_19.PDF", url.getFileName());
        assertEquals("pdf", url.getExtension());
        assertFalse(url.isStoredCompressed());
        assertFalse(url.isDataCompressed());

        assertEquals(PDF, url.getMediaType());
    }

    @Test
    public void testGZipFactoriesFromSpec() throws IOException {

        MongoFileUrl url = MongoFileUrl
                .construct("mongofile:/home/myself/foo/activeusers_19.ZIP?52fb1e7b36707d6d13ebfda9#application/zip");
        assertNotNull(url);

        assertEquals(new ObjectId("52fb1e7b36707d6d13ebfda9"), url.getMongoFileId());
        assertEquals("/home/myself/foo/activeusers_19.ZIP", url.getFilePath());
        assertEquals("activeusers_19.ZIP", url.getFileName());
        assertEquals("zip", url.getExtension());
        assertFalse(url.isStoredCompressed());
        assertTrue(url.isDataCompressed());

        assertEquals(ZIP, url.getMediaType());
    }

}
