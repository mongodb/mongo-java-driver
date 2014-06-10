package org.mongodb.file;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.file.util.BytesCopier;
import org.mongodb.file.writing.BufferedChunksOutputStream;

public class BufferedChunkOutputStreamTest {

    public static final Logger LOGGER = Loggers.getLogger("file");

    @Test
    public void test8KBuffer256kChunks() throws IOException {

        LogDumpOutputStreamSink log = new LogDumpOutputStreamSink();

        BufferedChunksOutputStream stream = new BufferedChunksOutputStream(log);
        try {
            new BytesCopier(8 * 1024, new ByteArrayInputStream(LoremIpsum.getBytes()), stream).transfer(false);
        } finally {
            stream.close();
        }

        // System.out.println(log.info());
        assertEquals(LoremIpsum.getString().length(), log.total);
        assertEquals(//
                "total = 32087, commands = [write(b, 0, 8192), write(b, 0, 8192), write(b, 0, 8192), write(b, 0, 7511), flush, close]", //
                log.info());
    }

    @Test
    public void test16KBuffers5kChunks() throws IOException {

        LogDumpOutputStreamSink log = new LogDumpOutputStreamSink();

        BufferedChunksOutputStream stream = new BufferedChunksOutputStream(log, 5 * 1024);
        try {
            new BytesCopier(16 * 1024, new ByteArrayInputStream(LoremIpsum.getBytes()), stream).transfer(false);
        } finally {
            stream.close();
        }

        LOGGER.debug("LoremIpsum length = " + LoremIpsum.getString().length());
        // System.out.println(log.info());
        assertEquals(LoremIpsum.getString().length(), log.total);
        assertEquals(
        //
                "total = 32087, commands = [write(b, 0, 5120), write(b, 0, 5120), write(b, 0, 5120), write(b, 0, 5120), "
                        + "write(b, 0, 5120), write(b, 0, 5120), write(b, 0, 1367), flush, close]", log.info());
    }

    @Test
    public void test13KBuffers15kChunks() throws IOException {

        LogDumpOutputStreamSink log = new LogDumpOutputStreamSink();

        BufferedChunksOutputStream stream = new BufferedChunksOutputStream(log, 15 * 1024);
        try {
            new BytesCopier(13 * 1024, new ByteArrayInputStream(LoremIpsum.getBytes()), stream).transfer(false);
        } finally {
            stream.close();
        }

        // System.out.println(log.info());
        assertEquals(LoremIpsum.getString().length(), log.total);
        assertEquals(
        //
                "total = 32087, commands = [write(b, 0, 15360), write(b, 0, 15360), write(b, 0, 1367), flush, close]", log.info());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalChunkSize() throws IOException {

        BufferedChunksOutputStream stream = new BufferedChunksOutputStream(null, -1);
        stream.close();
    }

    @Test
    public void testSingleByteWrite() throws IOException {

        LogDumpOutputStreamSink log = new LogDumpOutputStreamSink();

        OutputStream out = new BufferedChunksOutputStream(log, 5);
        try {
            out.write(Byte.valueOf("1"));
        } finally {
            out.close();
        }

        // System.out.println(log.info());

        assertEquals(1, log.total);
        assertEquals(
        //
                "total = 1, commands = [write(b, 0, 1), flush, close]", //
                log.info());
    }

    private class LogDumpOutputStreamSink extends OutputStream {

        private List<String> commands = new ArrayList<String>();
        private long total = 0;

        public String info() {

            return String.format("total = %d, commands = %s", total, commands.toString());
        }

        @Override
        public void write(final int b) throws IOException {

            commands.add("write(b)");
            ++total;
        }

        @Override
        public void write(final byte[] b) throws IOException {

            commands.add(String.format("write(b) - length = %d", b.length));
            total += b.length;
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {

            commands.add(String.format("write(b, %d, %d)", off, len));
            total += len;
        }

        @Override
        public void flush() throws IOException {

            commands.add("flush");
        }

        @Override
        public void close() throws IOException {

            commands.add("close");
        }

    }

}
