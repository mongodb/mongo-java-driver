/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author scott hernandez
 */
public class MapperOptionsTest extends TestBase {

    @SuppressWarnings("unused")
    private static class HasList implements Serializable {
        static final long serialVersionUID = 1L;
        @Id
        private final ObjectId id = new ObjectId();
        private List<String> names = new ArrayList<String>();


        HasList() {
        }

        HasList(final String n) {
            if (n == null) {
                names = null;
            }
            else {
                names.add(n);
            }
        }
    }

    @Test
    public void emptyListStoredWithOptions() {
        final HasList hl = new HasList();

        //Test default behavior
        ds.save(hl);
        DBObject dbObj = ds.getCollection(HasList.class).findOne();
        Assert.assertFalse("field exists, value =" + dbObj.get("names"), dbObj.containsField("names"));

        //Test default storing empty list/array with storeEmpties option
        this.morphia.getMapper().getOptions().storeEmpties = true;
        ds.save(hl);
        dbObj = ds.getCollection(HasList.class).findOne();
        Assert.assertTrue("field missing", dbObj.containsField("names"));

        //Test opposite from above
        this.morphia.getMapper().getOptions().storeEmpties = false;
        ds.save(hl);
        dbObj = ds.getCollection(HasList.class).findOne();
        Assert.assertFalse("field exists, value =" + dbObj.get("names"), dbObj.containsField("names"));
    }
}
