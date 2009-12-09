// DBMessageLayer.java

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

import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

public class DBMessageLayer extends DBApiLayer {
    
    DBMessageLayer( String root , DBConnector connector ){
        super( root );
        _connector = connector;
    }
    
    protected void doInsert( ByteBuffer buf , WriteConcern concern )
        throws MongoException {
        _connector.say( 2002 , buf , concern );
    }
    protected  void doDelete( ByteBuffer buf , WriteConcern concern ) 
        throws MongoException {
        _connector.say( 2006 , buf , concern );
    }
    protected void doUpdate( ByteBuffer buf , WriteConcern concern )
        throws MongoException {
        _connector.say( 2001 , buf , concern );
    }
    protected void doKillCursors( ByteBuffer buf )
        throws MongoException {
        _connector.say( 2007 , buf , WriteConcern.NORMAL );
    }
    
    protected int doQuery( ByteBuffer out , ByteBuffer in )
        throws MongoException {
        return _connector.call( 2004 , out , in );
    }
    protected int doGetMore( ByteBuffer out , ByteBuffer in )
        throws MongoException {
        return _connector.call( 2005 , out , in );
    }

    final DBConnector _connector;

}
