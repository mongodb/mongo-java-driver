package com.mongodb.samples;

import java.util.*;
import java.net.*;
import java.io.*;

import com.mongodb.*;
import com.mongodb.io.*;

import org.apache.commons.cli.*;

/**
 * An example of a paired db
 */
public class Paired {

    public static void main(String[] args ) 
        throws Exception{

        Options o = new Options();
        o.addOption( "P" , "path" , true , "path to db executable" );
        CommandLine cl = ( new BasicParser() ).parse( o , args );

        String path = cl.getOptionValue( "P" );
        if( path == null )
            return;

        Process arbP = Runtime.getRuntime().exec( path + "/db --port " + _arbPort );

        Process db1P = Runtime.getRuntime().exec( path + "/db --port " + _db1Port +
                                            " --pairwith 127.0.0.1:" + _db2Port + " 127.0.0.1:" + _arbPort );
 
        Process db2P = Runtime.getRuntime().exec( path + "/db --port " + _db2Port +
                                            " --pairwith 127.0.0.1:" + _db1Port + " 127.0.0.1:" + _arbPort );

        System.out.println( "Started dbs." );
        db2P.waitFor();
        db1P.waitFor();
        arbP.waitFor();
        /*
        ArrayList<DBAddress> list = new ArrayList<DBAddress>();
        list.add( new DBAddress( "127.0.0.1:" + _arbPort + "/ptest" ) );
        list.add( new DBAddress( "127.0.0.1:" + _db1Port + "/ptest" ) );
        list.add( new DBAddress( "127.0.0.1:" + _db2Port + "/ptest" ) );

        DBTCP dbtcp = new DBTCP( list );
        System.out.println( "connection point: " + dbtcp.getConnectPoint() );
        System.out.println( "address: " + dbtcp.getAddress() );

        System.out.println("start");

        dbtcp.requestEnsureConnection();
        dbtcp.requestStart();
        dbtcp.requestDone();

        System.out.println("done");

        arbP.destroy();
        db1P.destroy();
        db2P.destroy();
        */
    }

    static String _arbPort = "27015";
    static String _db1Port = "27016";
    static String _db2Port = "27017";

}