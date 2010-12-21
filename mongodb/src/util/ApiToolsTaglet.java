// ApiToolsTaglet.java

import java.util.*;
import com.sun.javadoc.*;
import com.sun.tools.doclets.*;

public class ApiToolsTaglet implements Taglet {

    public static void register( Map tagletMap ){
        ApiToolsTaglet t = new ApiToolsTaglet();
        tagletMap.put( t.getName() , t );
    }
    
    public String getName(){
        return "dochub";
    }
    
    public boolean inConstructor(){ return true; }
    public boolean inField(){ return true; }
    public boolean inMethod(){ return true; }
    public boolean inOverview(){ return true; }
    public boolean inPackage(){ return true; }
    public boolean inType(){ return true; }

    public boolean isInlineTag(){ return false; }

    String genLink( String name ){
        return new StringBuilder()
            .append( "<a href='http://dochub.mongodb.org/core/" ).append( name ).append( "' " )
            .append( "name='" ).append( name ).append( "' " )
            .append( ">").append( name ).append( "</a>" )
            .toString();
    }

    public String toString( Tag tag ){
        return toString( new Tag[]{ tag } );
    }
    
    public String toString( Tag[] tags ){
        if ( tags.length == 0 )
            return null;
        
        StringBuilder buf = new StringBuilder( "\n<br><DT><B>MongoDB Doc Links</B><DD>" );
        buf.append( "<ul>" );
        for ( Tag t : tags ){
            buf.append( "<li>" ).append( genLink( t.text() ) ).append( "</li>" );
        }
        buf.append( "</ul>" );
        buf.append( "</DD>\n" );
        return buf.toString();
    }


}
