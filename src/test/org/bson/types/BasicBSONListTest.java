/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.types;

import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class BasicBSONListTest {

    /*
     * EmptyKey:
     *
     * c-driver
     * bson_append_array_begin( &bson, "array", -1, &bson_array );
     * bson_append_int32( &bson_array, "", -1, 1 );
     * bson_append_int32( &bson_array, "", -1, 2 );
     * bson_append_array_end( &bson, &bson_array );
     *
     * view:
     * mongo cli work
     * csharp-driver work
     */

    @Test
    public void testPutEmptyKey() throws Exception {
        BasicBSONList   list = new BasicBSONList();
        try
        {
            list.put( "", 1 );
        }
        catch ( IllegalArgumentException e )
        {
            fail();
        }

        assertEquals( 1, list.size() );
        assertEquals( 1, list.get( 0 ) );
    }

    @Test
    public void testPutEmptyKeySequential() throws Exception {
        BasicBSONList   list = new BasicBSONList();
        try
        {
            list.put( "", 1 );
            list.put( "", 2 );
            list.put( "", 3 );
        }
        catch ( IllegalArgumentException e )
        {
            fail();
        }

        assertEquals( 3, list.size() );
        assertEquals( 1, list.get( 0 ) );
        assertEquals( 3, list.get( 2 ) );
        assertEquals( 2, list.get( 1 ) );
    }



    @Test
    public void testPutSequentialKeySequential() throws Exception {
        BasicBSONList   list = new BasicBSONList();
        try
        {
            list.put( "0", 1 );
            list.put( "1", 2 );
            list.put( "2", 3 );
        }
        catch ( IllegalArgumentException e )
        {
            fail();
        }

        assertEquals( 3, list.size() );
        assertEquals( 1, list.get( 0 ) );
        assertEquals( 3, list.get( 2 ) );
        assertEquals( 2, list.get( 1 ) );
    }

    @Test
    public void testPutNonSequentialKeySequential() throws Exception {
        BasicBSONList   list = new BasicBSONList();
        try
        {
            list.put( "0", 1 );
            list.put( "2", 2 );
            list.put( "1", 3 );
        }
        catch ( IllegalArgumentException e )
        {
            fail();
        }

        assertEquals( 3, list.size() );
        assertEquals( 1, list.get( 0 ) );
        assertEquals( 2, list.get( 2 ) );
        assertEquals( 3, list.get( 1 ) );
    }
}
