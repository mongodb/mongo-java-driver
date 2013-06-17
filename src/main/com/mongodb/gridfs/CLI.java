// CLI.java

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

package com.mongodb.gridfs;

import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.util.Util;

import java.io.File;
import java.security.DigestInputStream;
import java.security.MessageDigest;


/**
 * a simple CLI for Gridfs
 */
public class CLI {
    
    /**
     *  Dumps usage info to stdout
     */
    private static void printUsage() {
        System.out.println("Usage : [--db database] action");
        System.out.println("  where  action is one of:");
        System.out.println("      list                      : lists all files in the store");
        System.out.println("      put filename              : puts the file filename into the store");
        System.out.println("      get filename1 filename2   : gets filename1 from store and sends to filename2");
        System.out.println("      md5 filename              : does an md5 hash on a file in the db (for testing)");
    }

    private static String db = "test";
    private static String uri = "mongodb://127.0.0.1";
    private static Mongo _mongo = null;

    @SuppressWarnings("deprecation")
    private static Mongo getMongo()
        throws Exception {
        if ( _mongo == null )  {
            _mongo = new MongoClient(new MongoClientURI(uri));
        }
        return _mongo;
    }
    
    private static GridFS _gridfs;
    private static GridFS getGridFS()
        throws Exception {
        if ( _gridfs == null )
            _gridfs = new GridFS( getMongo().getDB( db ) );
        return _gridfs;
    }

    public static void main(String[] args) throws Exception {
        
        if ( args.length < 1 ){
            printUsage();
            return;
        }
        
        for ( int i=0; i<args.length; i++ ){
            String s = args[i];
            
            if ( s.equals( "--db" ) ){
                db = args[i+1];
                i++;
                continue;
            }

            if ( s.equals( "--host" ) ){
                uri = "mongodb://" + args[i+1];
                i++;
                continue;
            }

            if ( s.equals( "--uri" ) ){
                uri = args[i+1];
                i++;
                continue;
            }

            if ( s.equals( "help" ) ){
                printUsage();
                return;
            }
            
            if ( s.equals( "list" ) ){
                GridFS fs = getGridFS();
                
                System.out.printf("%-60s %-10s\n", "Filename", "Length");
                
                for ( DBObject o : fs.getFileList() ){
                    System.out.printf("%-60s %-10d\n", o.get("filename"), ((Number) o.get("length")).longValue());
                }
                return;
            }
            
            if ( s.equals( "get" ) ){
                GridFS fs = getGridFS();
                String fn = args[i+1];
                GridFSDBFile f = fs.findOne( fn );
                if ( f == null ){
                    System.err.println( "can't find file: " + fn );
                    return;
                }

                f.writeTo( f.getFilename() );
                return;
            }

            if ( s.equals( "put" ) ){
                GridFS fs = getGridFS();
                String fn = args[i+1];
                GridFSInputFile f = fs.createFile( new File( fn ) );
                f.save();
                f.validate();
                return;
            }
            

            if ( s.equals( "md5" ) ){
                GridFS fs = getGridFS();
                String fn = args[i+1];
                GridFSDBFile f = fs.findOne( fn );
                if ( f == null ){
                    System.err.println( "can't find file: " + fn );
                    return;
                }

                MessageDigest md5 = MessageDigest.getInstance("MD5");
                md5.reset();
                DigestInputStream is = new DigestInputStream( f.getInputStream() , md5 );
                int read = 0;
                while ( is.read() >= 0 ){ 
                    read++;
                    int r = is.read( new byte[17] ); 
                    if ( r < 0 )
                        break;
                    read += r;
                }
                byte[] digest = md5.digest();
                System.out.println( "length: " + read + " md5: " + Util.toHex( digest ) );
                return;
            }
            
            
            System.err.println( "unknown option: " + s );
            return;
        }
        
    }

}
