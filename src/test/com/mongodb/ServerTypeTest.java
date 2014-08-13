package com.mongodb;

import com.mongodb.util.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;

public class ServerTypeTest extends TestCase {
    @Test
    public void testGetServerType_MongoClient() throws Exception {
        ServerType type = ServerType.getServerType(getMongoClient());
        assertNotNull(type);
    }

    @Test
    public void testGetServerType_DB() throws Exception {
        ServerType type = ServerType.getServerType(getDatabase());
        assertNotNull(type);
    }
}