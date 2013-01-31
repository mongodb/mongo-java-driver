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

package com.google.code.morphia;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.testmodel.Circle;
import com.google.code.morphia.testmodel.Rectangle;
import com.google.code.morphia.testmodel.Shape;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class MapReduceTest extends TestBase {

    @Entity("mr-results")
    private static class ResultEntity extends ResultBase<String, HasCount> {
    }

    private static class ResultBase<T, V> {
        @Id
        private T type;
        @Embedded
        V value;
    }

    private static class HasCount {
        private double count;
    }

    @SuppressWarnings("unused")
    @Entity("mr-results")
    private static class ResultEntity2 {
        @Id
        private String type;
        private double count;

        @PreLoad
        void preLoad(final BasicDBObject dbObj) {
            //pull all the fields from value field into the parent.
            dbObj.putAll((DBObject) dbObj.get("value"));
        }
    }


    @Test
    public void testMR() throws Exception {

        final Random rnd = new Random();

        //create 100 circles and rectangles
        for (int i = 0; i < 100; i++) {
            ads.insert("shapes", new Circle(rnd.nextDouble()));
            ads.insert("shapes", new Rectangle(rnd.nextDouble(), rnd.nextDouble()));
        }
        final String map = "function () { if(this['radius']) { emit('circle', {count:1}); return; } emit('rect', "
                           + "{count:1}); }";
        final String reduce = "function (key, values) { var total = 0; for ( var i=0; i<values.length; i++ ) {total "
                              + "+= values[i].count;} return { count : total }; }";
        final MapreduceResults<ResultEntity> mrRes = ds.mapReduce(MapreduceType.REPLACE, ads.createQuery(Shape.class),
                                                                 map, reduce, null, null, ResultEntity.class);
        Assert.assertEquals(2, mrRes.createQuery().countAll());
        Assert.assertEquals(100, mrRes.createQuery().get().value.count, 0);
    }

}
