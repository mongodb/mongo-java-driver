// DBBSONEncoder.java

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

import java.util.*;
import java.util.regex.*;
import java.util.concurrent.atomic.*;
import java.nio.*;
import java.nio.charset.*;
import java.lang.reflect.Array;

import com.mongodb.util.*;

import org.bson.*;
import org.bson.types.*;
import static org.bson.BSON.*;

public class DBBSONEncoder extends BSONEncoder {
    
    protected boolean handleSpecialObjects( String name , BSONObject o ){
        
        if ( o == null )
            return false;

        if ( o instanceof DBCollection ){
            DBCollection c = (DBCollection)o;
            putDBPointer( name , c.getName() , Bytes.COLLECTION_REF_ID );
            return true;
        }
        
        if ( name != null && o instanceof DBPointer ){
            DBPointer r = (DBPointer)o;
            putDBPointer( name , r._ns , (ObjectId)r._id );
            return true;
        }
        
        return false;
    }

    protected boolean putSpecial( String name , Object val ){
        if ( val instanceof DBPointer ){
            DBPointer r = (DBPointer)val;
            putDBPointer( name , r._ns , (ObjectId)r._id );
            return true;
        }
        
        if ( val instanceof DBRefBase ){
            putDBRef( name, (DBRefBase)val );
            return true;
        }
        
        return false;
    }

    protected void putDBPointer( String name , String ns , ObjectId oid ){
        _put( REF , name );
        
        _putValueString( ns );
        _buf.writeInt( oid._time() );
        _buf.writeInt( oid._machine() );
        _buf.writeInt( oid._inc() );
    }

    protected void putDBRef( String name, DBRefBase ref ){
        _put( OBJECT , name );
        final int sizePos = _buf.getPosition();
        _buf.writeInt( 0 );
        
        _putObjectField( "$ref" , ref.getRef() );
        _putObjectField( "$id" , ref.getId() );

        _buf.write( EOO );
        _buf.writeInt( sizePos , _buf.getPosition() - sizePos );
    }


}
