// MongoURITest.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;


public class MongoURITest extends TestCase {

    @Test()
    public void testBasic1(){
        MongoURI u = new MongoURI( "mongodb://foo/bar" );
        assertEquals( 1 , u.getHosts().size() );
        assertEquals( "foo" , u.getHosts().get(0) );
        assertEquals( "bar" , u.getDatabase() );
        assertEquals( null , u.getCollection() );
        assertEquals( null , u.getUsername() );
        assertEquals( null , u.getPassword() );
    }

    @Test()
    public void testBasic2(){
        MongoURI u = new MongoURI( "mongodb://foo/bar.goo" );
        assertEquals( 1 , u.getHosts().size() );
        assertEquals( "foo" , u.getHosts().get(0) );
        assertEquals( "bar" , u.getDatabase() );
        assertEquals( "goo" , u.getCollection() );
    }

    @Test()
    public void testUserPass(){
        MongoURI u = new MongoURI( "mongodb://user:pass@host/bar" );
        assertEquals( 1 , u.getHosts().size() );
        assertEquals( "host" , u.getHosts().get(0) );
        assertEquals( "user" , u.getUsername() );
        assertEquals( "pass" , new String( u.getPassword() ) );
    }

    @Test()
    public void testUserPassAndPort(){
        MongoURI u = new MongoURI( "mongodb://user:pass@host:27011/bar" );
        assertEquals( 1 , u.getHosts().size() );
        assertEquals( "host:27011" , u.getHosts().get(0) );
        assertEquals( "user" , u.getUsername() );
        assertEquals( "pass" , new String( u.getPassword() ) );
    }

    @Test()
    public void testUserPassAndMultipleHostsWithPort(){
        MongoURI u = new MongoURI( "mongodb://user:pass@host:27011,host2:27012,host3:27013/bar" );
        assertEquals( 3 , u.getHosts().size() );
        assertEquals( "host:27011" , u.getHosts().get(0) );
        assertEquals( "host2:27012" , u.getHosts().get(1) );
        assertEquals( "host3:27013" , u.getHosts().get(2) );
        assertEquals( "user" , u.getUsername() );
        assertEquals( "pass" , new String( u.getPassword() ) );
    }


    @Test()
    public void testOptions(){
        MongoURI uAmp = new MongoURI( "mongodb://localhost/test?" +
                        "maxPoolSize=10&waitQueueMultiple=5&waitQueueTimeoutMS=150&" +
                        "connectTimeoutMS=2500&socketTimeoutMS=5500&autoConnectRetry=true&" +
                        "slaveOk=true&safe=false&w=1&wtimeout=2500&fsync=true");
        _testOpts( uAmp._options );
        MongoURI uSemi = new MongoURI( "mongodb://localhost/test?" +
                "maxPoolSize=10;waitQueueMultiple=5;waitQueueTimeoutMS=150;" +
                "connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;" +
                "slaveOk=true;safe=false;w=1;wtimeout=2500;fsync=true");
        _testOpts( uSemi._options );
        MongoURI uMixed = new MongoURI( "mongodb://localhost/test?" +
                "maxPoolSize=10&waitQueueMultiple=5;waitQueueTimeoutMS=150;" +
                "connectTimeoutMS=2500;socketTimeoutMS=5500&autoConnectRetry=true;" +
                "slaveOk=true;safe=false&w=1;wtimeout=2500;fsync=true");
        _testOpts( uMixed._options );
    }

    @Test()
    public void testReadPreferenceOptions(){
        MongoURI uri = new MongoURI("mongodb://localhost/?readPreference=secondaryPreferred");
        assertEquals(ReadPreference.secondaryPreferred(), uri._options.readPreference);

        uri = new MongoURI("mongodb://localhost/?readPreference=secondaryPreferred&" +
                "readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=");
        assertEquals(ReadPreference.secondaryPreferred
                (
                        new BasicDBObject("dc", "ny").append("rack", "1"),
                        new BasicDBObject("dc", "ny"),
                        new BasicDBObject()
                ),
                uri._options.readPreference);
    }

    @SuppressWarnings("deprecation")
    private void _testOpts(MongoOptions uOpt){
        assertEquals( uOpt.connectionsPerHost, 10 );
        assertEquals( uOpt.threadsAllowedToBlockForConnectionMultiplier, 5 );
        assertEquals( uOpt.maxWaitTime, 150 );
        assertEquals( uOpt.socketTimeout, 5500 );
        assertTrue( uOpt.autoConnectRetry );
        assertTrue( uOpt.slaveOk );
        assertFalse( uOpt.safe );
        assertEquals( uOpt.w, 1 );
        assertEquals( uOpt.wtimeout, 2500 );
        assertTrue( uOpt.fsync );
        assertEquals( uOpt.getWriteConcern(), new WriteConcern(1, 2500, true) );
        assertEquals( null, uOpt.readPreference);
    }
    public static void main( String args[] )
        throws Exception {
        (new MongoURITest()).runConsole();

    }

}
