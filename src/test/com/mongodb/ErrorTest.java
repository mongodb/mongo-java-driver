/**
 *  See the NOTICE.txt file distributed with this work for
 *  information regarding copyright ownership.
 *
 *  The authors license this file to you under the
 *  Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License.  You may
 *  obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.mongodb;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

/**
 *
 */
public class ErrorTest {

    Mongo _db;

    @BeforeClass
    public void setUp() throws Exception{
        _db = new Mongo("com_mongodb_ErrorTest");
    }

    @Test
    public void testLastError() {

        assert(_db.getLastError().get("err") == null);

        _db.forceError();

        assert(_db.getLastError().get("err") != null);

        _db.resetError();
        assert(_db.getLastError().get("err") == null);
    }

    @Test
    public void testPrevError() {

        _db.resetError();
        
        assert(_db.getLastError().get("err") == null);
        assert(_db.getPreviousError().get("err") == null);

        _db.forceError();

        assert(_db.getLastError().get("err") != null);
        assert(_db.getPreviousError().get("err") != null);

        _db.getCollection("misc").insert(new BasicDBObject("foo", 1));

        assert(_db.getLastError().get("err") == null);
        assert(_db.getPreviousError().get("err") != null);

        _db.resetError();

        assert(_db.getLastError().get("err") == null);
        assert(_db.getPreviousError().get("err") == null);
    }
}
