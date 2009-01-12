package com.mongodb;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.net.*;

public class Wiki {
    public static void saveToWiki( DBCollection collection ) {
        DBObject obj1 = new BasicDBObject();
        System.out.println( "Create first object: \n"+ 
                            "\tname: \"Groundhog\"\n" +
                            "\ttext: \"Suited to their temperate habitat, groundhogs...\"" );
        obj1.put( "name", "Groundhog" );
        obj1.put( "text", "Suited to their temperate habitat, groundhogs are covered with two coats of fur: a dense grey undercoat and a longer coat of banded guard hairs that gives the groundhog its distinctive \"frosted\" appearance.\n" );

        System.out.println( "Saving first object to db" );
        collection.save( obj1 );

        System.out.println( "Create second object: \n"+ 
                            "\tname: \"Hedgehog\"\n" +
                            "\ttext: \"Hedgehogs are easily recognized by their spines...\"" );
        DBObject obj2 = new BasicDBObject();
        obj2.put( "name", "Hedgehog" );
        obj2.put( "text", "Hedgehogs are easily recognized by their spines, which are hollow hairs made stiff with keratin.\n" );

        System.out.println( "Saving second object to db" );
        collection.save( obj2 );
    }

    public static void writeWikiToFiles( DBCollection collection ) {
        Iterator<DBObject> c = collection.find();
        while( c.hasNext() ) {
            DBObject obj = c.next();
            String name = obj.get( "name" ).toString();
            String text = obj.get( "text" ).toString();
            try {
                FileWriter fw = new FileWriter( new File( "./" + name + ".out" ) );
                fw.write( text, 0, text.length() );
                fw.close();
                System.out.println("\twrote "+name+".out");
            }
            catch( Exception e ) {
                System.out.println("\terror: "+name );
                e.printStackTrace();
            }
        }
    }


    public static void main( String[] args ) {
        Mongo m;
        String address = "127.0.0.1:27017/samples";
        try {
            m = new Mongo( new DBAddress( address ) );
        }
        catch( UnknownHostException e ) {
            System.out.println( "Couldn't find db address host: "+address );
            e.printStackTrace();
            return;
        }

        DBCollection collection = m.getCollection( "wiki" );
        collection.drop();

        System.out.println("Saving 2 objects to wiki: ");
        saveToWiki( collection );
        System.out.println("Write db wiki entries to files: ");
        writeWikiToFiles( collection );
        System.out.println( "Done!" );            
    }
}
