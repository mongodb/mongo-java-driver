package org.mongodb.file.url;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.file.MongoFileTest;

public class MongoFileProtocolTest {

    @Test
    public void testKnown() {

        MongoFileURLStreamHandlerFactory factory = new MongoFileURLStreamHandlerFactory();
        URLStreamHandler x = factory.createURLStreamHandler(MongoFileUrl.PROTOCOL);
        assertNotNull(x);
        assertEquals(x.getClass().getName(), Handler.class.getName());
    }

    @Test
    public void testUnknown() {

        MongoFileURLStreamHandlerFactory factory = new MongoFileURLStreamHandlerFactory();
        URLStreamHandler x = factory.createURLStreamHandler("foobar");
        assertNull(x);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testHandlerOpenConnection() throws IOException {

        ObjectId id = new ObjectId();
        MongoFileUrl url = MongoFileUrl.construct(id, "fileName.pdf", MongoFileTest.PDF, true);
        URLConnection connection = url.getUrl().openConnection();

        fail("This test should throw an exception");
        assert connection != null; // dummy line to get rid of unused warning
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testHandlerGetContent() throws IOException {

        ObjectId id = new ObjectId();
        MongoFileUrl url = MongoFileUrl.construct(id, "fileName.pdf", MongoFileTest.PDF, true);
        Object object = url.getUrl().getContent();

        fail("This test should throw an exception");
        assert object != null; // dummy line to get rid of unused warning
    }

}
