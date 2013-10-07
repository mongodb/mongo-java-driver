// TestCase.java

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

package com.mongodb.util;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import org.testng.annotations.AfterClass;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class TestCase extends MyAsserts {

    static class Test {
        Test( Object o , Method m ){
            _o = o;
            _m = m;
        }
        
        Result run(){
            try {
                _m.invoke( _o );
                return new Result( this );
            }
            catch ( IllegalAccessException e ){
                return new Result( this , e );
            }
            catch ( InvocationTargetException ite ){
                return new Result( this , ite.getTargetException() );
            }
        }

        public String toString(){
            String foo = _o.getClass().getName() + "." + _m.getName();
            if ( _name == null )
                return foo;
            return _name + "(" + foo + ")";
        }

        protected String _name = null;

        final Object _o;
        final Method _m;
    }

    static class Result {
        Result( Test t ){
            this( t , null );
        }

        Result( Test t , Throwable error ){
            _test = t;
            _error = error;
        }

        boolean ok(){
            return _error == null;
        }

        public String toString(){
            StringBuilder buf = new StringBuilder();
            buf.append( _test );
            Throwable error = _error;
            while ( error != null ){
                buf.append( "\n\t" + error + "\n" );
                for ( StackTraceElement ste : error.getStackTrace() ){
                    buf.append( "\t\t" + ste + "\n" );
                }
                error = error.getCause();
            }
            return buf.toString();
        }
        
        final Test _test;
        final Throwable _error;
    }


    /**
     * this is for normal class tests
     */

    public TestCase(){ 
        this( null );
    }

    public TestCase( String name ) {
        if (staticMongoClient == null) {
            try {
                staticMongoClient = new MongoClient();
                staticMongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        cleanupMongo = staticMongoClient;

        for ( Method m : getClass().getMethods() ){
            
            if ( ! m.getName().startsWith( "test" ) )
                continue;
            
            if ( ( m.getModifiers() & Member.PUBLIC ) > 0 )
                continue;

            Test t = new Test( this , m );
            t._name = name;
            _tests.add( t );
            
        }
    }

    public TestCase( Object o , String m )
        throws NoSuchMethodException {
        this( o , o.getClass().getDeclaredMethod( m ) );
    }

    public TestCase( Object o , Method m ){
        _tests.add( new Test( o , m ) );
    }

    public void add( TestCase tc ){
        _tests.addAll( tc._tests );
    }

    public String cleanupDB = null;
    public Mongo cleanupMongo = null;

    private static MongoClient staticMongoClient;

    @AfterClass
    public void cleanup() {
        if (cleanupMongo != null) {
            if (cleanupDB != null) {
                cleanupMongo.dropDatabase(cleanupDB);
            }
        }
    }

    /**
     * @return true if everything succeeds
     */
    public boolean runConsole(){
        List<Result> errors = new ArrayList<Result>();
        List<Result> fails = new ArrayList<Result>();

        System.out.println( "Num Tests : " + _tests.size() );
        System.out.println( "----" );

        for ( Test t : _tests ){
            Result r = t.run();
            if ( r.ok() ){
                System.out.print(".");
                continue;
            }
            
            System.out.print( "x" );

            if ( r._error instanceof MyAssert )
                fails.add( r );
            else
                errors.add( r );
        }
        cleanup();
        System.out.println( "\n----" );

        int pass = _tests.size() - ( errors.size() + fails.size() );

        System.out.println( "Passes : " + pass + " / " + _tests.size() );
        System.out.println( "% Pass : " + ( ((double)pass*100) / _tests.size() ) );
        if ( pass == _tests.size() ){
            System.out.println( "SUCCESS" );
            return true;
        }
        
        System.err.println( "Num Pass : " + ( _tests.size() - ( errors.size() + fails.size() ) ) );
        System.err.println( "Num Erros : " + (  errors.size() ) );
        System.err.println( "Num Fails : " + (  fails.size() ) );

        System.err.println( "---------" );
        System.err.println( "ERRORS" );
        for ( Result r : errors )
            System.err.println( r );
        
        System.err.println( "---------" );
        System.err.println( "FAILS" );
        for ( Result r : fails )
            System.err.println( r );
        
        return false;
    }

    public String toString(){
        return  "TestCase numCase:" + _tests.size();
    }
    
    final List<Test> _tests = new ArrayList<Test>();
    
    protected static void run( String args[] ){
        Args a = new Args( args );
        
        boolean foundMe = false;
        String theClass = null;
        for ( StackTraceElement ste : Thread.currentThread().getStackTrace() ){
            if ( foundMe ){
                theClass = ste.getClassName();
                break;
            }
            
            if ( ste.getClassName().equals( "com.mongodb.util.TestCase" ) && 
                 ste.getMethodName().equals( "run" ) )
                foundMe = true;
        }
        
        if ( theClass == null )
            throw new RuntimeException( "something is broken" );
        
        try {
            TestCase tc = (TestCase) Class.forName( theClass ).newInstance();

            if ( a.getOption( "m" ) != null )
                tc = new TestCase( tc , a.getOption( "m" ) );
            
            tc.runConsole();
        }
        catch ( Exception e ){
            throw new RuntimeException( e );
        }
    }

    /**
     *
     * @param version  must be a major version, e.g. 1.8, 2,0, 2.2
     * @return true if server is at least specified version
     */
    protected boolean serverIsAtLeastVersion(double version) {
        String serverVersion = (String) cleanupMongo.getDB("admin").command("serverStatus").get("version");
        return Double.parseDouble(serverVersion.substring(0, 3)) >= version;
    }

    /**
     *
     * @param mongo the connection
     * @return true if connected to a standalone server
     */
    protected boolean isStandalone(Mongo mongo) {
        return runReplicaSetStatusCommand(mongo) == null;
    }

    @SuppressWarnings({"unchecked"})
    protected String getPrimaryAsString(Mongo mongo) {
        return getMemberNameByState(mongo, "primary");
    }

    @SuppressWarnings({"unchecked"})
    protected String getASecondaryAsString(Mongo mongo) {
        return getMemberNameByState(mongo, "secondary");
    }

    @SuppressWarnings({"unchecked"})
    protected String getMemberNameByState(Mongo mongo, String stateStrToMatch) {
        CommandResult replicaSetStatus = runReplicaSetStatusCommand(mongo);

        for (final BasicDBObject member : (List<BasicDBObject>) replicaSetStatus.get("members")) {
            String hostnameAndPort = member.getString("name");
            if (!hostnameAndPort.contains(":"))
                hostnameAndPort = hostnameAndPort + ":27017";

            final String stateStr = member.getString("stateStr");

            if (stateStr.equalsIgnoreCase(stateStrToMatch))
                return hostnameAndPort;
        }

        throw new IllegalStateException("No member found in state " + stateStrToMatch);
    }

    @SuppressWarnings("unchecked")
    protected int getReplicaSetSize(Mongo mongo) {
        int size = 0;

        CommandResult replicaSetStatus = runReplicaSetStatusCommand(mongo);

        for (final BasicDBObject member : (List<BasicDBObject>) replicaSetStatus.get("members")) {

            final String stateStr = member.getString("stateStr");

            if (stateStr.equals("PRIMARY") || stateStr.equals("SECONDARY"))
                size++;
        }

        return size;
    }

    
    protected CommandResult runReplicaSetStatusCommand(final Mongo pMongo) {
        // Check to see if this is a replica set... if not, get out of here.
        final CommandResult result = pMongo.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));

        final String errorMsg = result.getErrorMessage();

        if (errorMsg != null && errorMsg.indexOf("--replSet") != -1) {
            System.err.println("---- SecondaryReadTest: This is not a replica set - not testing secondary reads");
            return null;
        }

        return result;
    }



    public static void main( String args[] )
        throws Exception {
        
        String dir = "src/test";
        if ( args != null && args.length > 0 )
            dir = args[0];

        Process p = Runtime.getRuntime().exec( "find " + dir );
        BufferedReader in = new BufferedReader( new InputStreamReader( p.getInputStream() ) );

        TestCase theTestCase = new TestCase();
        
        String line;
        while ( ( line = in.readLine() ) != null ){

            if ( ! line.endsWith( "Test.java" ) ) {
                continue;
            }
        	
            line = line.substring( "src/test/".length() );        	
            line = line.substring( 0 , line.length() - ".java".length() );
            line = line.replaceAll( "//+" , "/" );
            line = line.replace( '/' , '.' );

            
            try {
		Class c = Class.forName( line );
		Object thing = c.newInstance();
		if ( ! ( thing instanceof TestCase ) )
		    continue;

		System.out.println( line );
	    
                TestCase tc = (TestCase)thing;
                theTestCase._tests.addAll( tc._tests );
            }
            catch ( Exception e ){
                e.printStackTrace();
            }
        }
        
        theTestCase.runConsole();
    }
}
