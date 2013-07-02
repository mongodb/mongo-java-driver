package com.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.mongodb.util.TestCase;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

public class QueryLogTest extends TestCase {

    public QueryLogTest() {
        _mongo = cleanupMongo;
        cleanupDB = "com_mongodb_unittest_QueryLogTest";
        _db = cleanupMongo.getDB( cleanupDB );
    }

    @Test
    public void testQueryLogs() throws Exception{
        DBCollection test = _db.getCollection("test");
        setUpLogger();
        test.save(new BasicDBObject("key1","val1"));
        test.save(new BasicDBObject("key2","val2"));

        int saveCount = 0;
        for(LogRecord lr : testHandler.getRecords()){
            //Check for logMessage with word 'save' as
            //we save two objects once loggers are enabled
            if(lr.getMessage().indexOf("save") != -1){
                saveCount++;
            }
        }

        //Should have 2 save records related to save operation
        assertEquals(2,saveCount);
    }

    @AfterTest
    public void cleanup(){
        DBApiLayer.TRACE_LOGGER.removeHandler(testHandler);
        DBApiLayer.TRACE_LOGGER.removeHandler(consoleHandler);
        testHandler.reset();
    }

    private void setUpLogger() {
        Logger logger = DBApiLayer.TRACE_LOGGER;
        logger.addHandler(testHandler);
        logger.setLevel(Level.FINEST);

        consoleHandler.setLevel(Level.FINEST);
        logger.addHandler(consoleHandler);
    }

    final Mongo _mongo;
    final DB _db;
    final TestHandler testHandler = new TestHandler();
    final ConsoleHandler consoleHandler = new ConsoleHandler();

    private static class TestHandler extends Handler {
        private final List<LogRecord> records = new ArrayList<LogRecord>();

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() throws SecurityException {
            records.clear();
        }

        public List<LogRecord> getRecords() {
            return records;
        }

        public void reset(){
            records.clear();
        }
    }
}
