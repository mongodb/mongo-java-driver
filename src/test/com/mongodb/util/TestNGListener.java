// TestNGListener.java

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
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.net.UnknownHostException;

public class TestNGListener extends TestListenerAdapter {

    public void onConfigurationFailure(ITestResult itr){
        super.onConfigurationFailure( itr );
        _print( itr.getThrowable() );
    }

    public void onTestFailure(ITestResult tr) {
        super.onTestFailure( tr );
        log("F");
    }

    public void onTestSkipped(ITestResult tr) {
        super.onTestSkipped( tr );
        log("S");
    }
    
    public void onTestSuccess(ITestResult tr) {
        super.onTestSuccess( tr );
        log(".");
    }

    private void log(String string) {
        System.out.print(string);
        if ( ++_count % 40 == 0) {
            System.out.println("");
        }
        System.out.flush();
    }

    public void onFinish(ITestContext context){
        System.out.println();

        for ( ITestResult r : context.getFailedTests().getAllResults() ){
            System.out.println(r);
            System.out.println("Exception : ");
            _print( r.getThrowable() );
        }

        try {
            _recordResults( context );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    private void _recordResults( ITestContext context ) throws UnknownHostException {
        DBObject obj = new BasicDBObject();
        for( ITestResult r : context.getPassedTests().getAllResults() ) {
            obj.put( (r.getTestClass().getName() + "." + r.getName()).replace('.', '_'),
                     r.getEndMillis()-r.getStartMillis() );
        }
        obj.put( "total", context.getEndDate().getTime()-context.getStartDate().getTime() );
        obj.put( "time", System.currentTimeMillis() );

        Mongo mongo = new MongoClient();
        try {
            mongo.getDB( "results" ).getCollection( "testng" ).save( obj );
        }
        catch( Exception e ) {
            System.err.println( "\nUnable to save test results to the db." );
            e.printStackTrace();
        } finally {
            mongo.close();
        }
    }

    private void _print( Throwable t ){

        int otcount = 0;
        int jlrcount = 0;

        if (t == null) {
            return;
        }

        System.out.println("-" + t.toString()+ "-");

        for ( StackTraceElement e : t.getStackTrace() ){
            if ( e.getClassName().startsWith( "org.testng.")) {
                if (otcount++ == 0) {
                    System.out.println("  " + e + " (with others of org.testng.* omitted)");
                }
            }
            else if (e.getClassName().startsWith( "java.lang.reflect.") || e.getClassName().startsWith("sun.reflect.") ) {
                if (jlrcount++ == 0) {
                    System.out.println("  " + e  + " (with others of java.lang.reflect.* or sun.reflect.* omitted)");
                }
            }
            else {
                System.out.println("  " +  e );
            }
        }

        if (t.getCause() != null) {
            System.out.println("Caused By : ");
        
            _print(t.getCause());
        }

        System.out.println();
    }

    private int _count = 0;
} 
