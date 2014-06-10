package org.mongodb.file.url;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URL;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.file.MongoFileTest;

public class ParserTest {

    @Test
    public void testAutoAssignedCompression() throws IOException {

        ObjectId id = new ObjectId();
        URL url = Parser.construct(id, "fileName.pdf", MongoFileTest.PDF, null, true);

        assertNotNull(url);
        assertEquals(String.format("mongofile:gz:fileName.pdf?%s#application/pdf", id.toString()), url.toString());

    }

    @Test
    public void testCustomAssignedCompression() throws IOException {

        ObjectId id = new ObjectId();
        URL url = Parser.construct(id, "fileName.pdf", MongoFileTest.PDF, "foo", true);

        assertNotNull(url);
        assertEquals(String.format("mongofile:foo:fileName.pdf?%s#application/pdf", id.toString()), url.toString());

    }

    @Test
    public void testNoCompression() throws IOException {

        ObjectId id = new ObjectId();
        URL url = Parser.construct(id, "fileName.zip", MongoFileTest.ZIP, null, true);

        assertNotNull(url);
        assertEquals(String.format("mongofile:fileName.zip?%s#application/zip", id.toString()), url.toString());

    }

    @Test
    public void testBlockedCompression() throws IOException {

        ObjectId id = new ObjectId();
        URL url = Parser.construct(id, "fileName.pdf", MongoFileTest.PDF, null, false);

        assertNotNull(url);
        assertEquals(String.format("mongofile:fileName.pdf?%s#application/pdf", id.toString()), url.toString());

    }

    @Test
    public void testCustomAssignedOverrideCompression() throws IOException {

        ObjectId id = new ObjectId();
        URL url = Parser.construct(id, "fileName.pdf", MongoFileTest.PDF, "foo", false);

        assertNotNull(url);
        assertEquals(String.format("mongofile:foo:fileName.pdf?%s#application/pdf", id.toString()), url.toString());

    }
}
