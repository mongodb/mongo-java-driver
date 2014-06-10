package org.mongodb.file;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.file.reading.CountingInputStream;
import org.mongodb.file.util.BytesCopier;

public class CountingInputStreamTest {

    private static final String LOREM_IPSUM = LoremIpsum.getString();

    @Test
    public void test() throws IOException {

        ByteArrayInputStream lStream = new ByteArrayInputStream(LOREM_IPSUM.getBytes());
        MongoFile mongoFile = new MongoFile(null, new Document());
        mongoFile.put(MongoFileConstants.length, LOREM_IPSUM.length());
        // Mockito.when(mongoFile.getLength()).thenReturn((long) LOREM_IPSUM.length());

        CountingInputStream in = new CountingInputStream(mongoFile, lStream);

        ByteArrayOutputStream out = new ByteArrayOutputStream(LOREM_IPSUM.length());
        new BytesCopier(in, out).transfer(true);
        // in.close();

        assertEquals(LOREM_IPSUM.length(), out.size());
        assertEquals(LOREM_IPSUM.length(), in.getCount());

    }

}
