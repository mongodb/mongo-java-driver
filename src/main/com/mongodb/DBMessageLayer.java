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

public abstract class DBMessageLayer extends DBApiLayer {
    
    DBMessageLayer( String root ){
        super( root );
    }

    protected void doInsert( ByteBuffer buf ){
        say( 2002 , buf );
    }
    protected  void doDelete( ByteBuffer buf ){
        say( 2006 , buf );
    }
    protected void doUpdate( ByteBuffer buf ){
        say( 2001 , buf );
    }
    protected void doKillCursors( ByteBuffer buf ){
        say( 2007 , buf );
    }
    
    protected int doQuery( ByteBuffer out , ByteBuffer in ){
        return call( 2004 , out , in );
    }
    protected int doGetMore( ByteBuffer out , ByteBuffer in ){
        return call( 2005 , out , in );
    }
    
    protected abstract void say( int op , ByteBuffer buf );
    protected abstract int call( int op , ByteBuffer out , ByteBuffer in );

}
