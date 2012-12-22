/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.cache.DefaultEntityCache;
import com.google.code.morphia.testmodel.Circle;
import com.google.code.morphia.testmodel.Rectangle;
import com.google.code.morphia.testmodel.Shape;
import com.google.code.morphia.testmodel.ShapeShifter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 *
 * @author Olafur Gauti Gudmundsson
 */
public class TestInterfaces extends TestBase {

    @Test
    public void testDynamicInstantiation() throws Exception {
        DBCollection shapes = db.getCollection("shapes");
        DBCollection shapeshifters = db.getCollection("shapeshifters");

        morphia.map(Circle.class)
                .map(Rectangle.class)
                .map(ShapeShifter.class);

        Shape rectangle = new Rectangle(2,5);
        
        DBObject rectangleDbObj = morphia.toDBObject(rectangle);
        shapes.save(rectangleDbObj);

        BasicDBObject rectangleDbObjLoaded = (BasicDBObject) shapes.findOne(new BasicDBObject(Mapper.ID_KEY, rectangleDbObj.get(Mapper.ID_KEY)));
		Shape rectangleLoaded = morphia.fromDBObject(Shape.class, rectangleDbObjLoaded, new DefaultEntityCache());

        assertTrue(rectangle.getArea() == rectangleLoaded.getArea());
        assertTrue(rectangleLoaded instanceof Rectangle);

        ShapeShifter shifter = new ShapeShifter();
        shifter.setReferencedShape(rectangleLoaded);
        shifter.setMainShape(new Circle(2.2));
        shifter.getAvailableShapes().add(new Rectangle(3,3));
        shifter.getAvailableShapes().add(new Circle(4.4));

        DBObject shifterDbObj = morphia.toDBObject(shifter);
        shapeshifters.save(shifterDbObj);

        BasicDBObject shifterDbObjLoaded = (BasicDBObject) shapeshifters.findOne(new BasicDBObject(Mapper.ID_KEY, shifterDbObj.get(Mapper.ID_KEY)));
		ShapeShifter shifterLoaded = morphia.fromDBObject(ShapeShifter.class, shifterDbObjLoaded, new DefaultEntityCache());

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
