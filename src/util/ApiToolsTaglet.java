// ApiToolsTaglet.java

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

import java.util.*;
import com.sun.javadoc.*;
import com.sun.tools.doclets.*;

public class ApiToolsTaglet implements Taglet {

    @SuppressWarnings("unchecked")
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
