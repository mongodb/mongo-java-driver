/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.cache.DefaultEntityCache;
import com.google.code.morphia.testmodel.Circle;
import com.google.code.morphia.testmodel.Rectangle;
import com.google.code.morphia.testmodel.Shape;
import com.google.code.morphia.testmodel.ShapeShifter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Olafur Gauti Gudmundsson
 */
public class InterfacesTest extends TestBase {

    @Test
    public void testDynamicInstantiation() throws Exception {
        final DBCollection shapes = db.getCollection("shapes");
        final DBCollection shapeshifters = db.getCollection("shapeshifters");

        morphia.map(Circle.class)
                .map(Rectangle.class)
                .map(ShapeShifter.class);

        final Shape rectangle = new Rectangle(2, 5);

        final DBObject rectangleDbObj = morphia.toDBObject(rectangle);
        shapes.save(rectangleDbObj);

        final BasicDBObject rectangleDbObjLoaded = (BasicDBObject) shapes.findOne(new BasicDBObject(Mapper.ID_KEY,
                                                                                                    rectangleDbObj
                                                                                                            .get


                                                                                                                    (Mapper.ID_KEY)));
        final Shape rectangleLoaded = morphia.fromDBObject(Shape.class, rectangleDbObjLoaded, new DefaultEntityCache());

        assertTrue(rectangle.getArea() == rectangleLoaded.getArea());
        assertTrue(rectangleLoaded instanceof Rectangle);

        final ShapeShifter shifter = new ShapeShifter();
        shifter.setReferencedShape(rectangleLoaded);
        shifter.setMainShape(new Circle(2.2));
        shifter.getAvailableShapes().add(new Rectangle(3, 3));
        shifter.getAvailableShapes().add(new Circle(4.4));

        final DBObject shifterDbObj = morphia.toDBObject(shifter);
        shapeshifters.save(shifterDbObj);

        final BasicDBObject shifterDbObjLoaded = (BasicDBObject) shapeshifters.findOne(new BasicDBObject(Mapper.ID_KEY, shifterDbObj.get(Mapper.ID_KEY)));
        final ShapeShifter shifterLoaded = morphia.fromDBObject(ShapeShifter.class, shifterDbObjLoaded, new DefaultEntityCache());

        assertNotNull(shifterLoaded);
        assertNotNull(shifterLoaded.getReferencedShape());
        assertNotNull(shifterLoaded.getReferencedShape().getArea());
        assertNotNull(rectangle);
        assertNotNull(rectangle.getArea());

        assertTrue(rectangle.getArea() == shifterLoaded.getReferencedShape().getArea());
        assertTrue(shifterLoaded.getReferencedShape() instanceof Rectangle);
        assertTrue(shifter.getMainShape().getArea() == shifterLoaded.getMainShape().getArea());
        assertEquals(shifter.getAvailableShapes().size(), shifterLoaded.getAvailableShapes().size());
    }
}
