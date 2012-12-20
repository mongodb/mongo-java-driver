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

package com.google.code.morphia.testmodel;

import java.util.HashSet;
import java.util.Set;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;

/**
 *
 * @author Olafur Gauti Gudmundsson
 */
@Entity("shapeshifters")
public class ShapeShifter extends TestEntity {
	private static final long serialVersionUID = 1L;

	@Embedded
    private Shape mainShape;

    @Reference
    private Shape referencedShape;

    @Embedded
    private Set<Shape> availableShapes;

    public ShapeShifter() {
        super();
        availableShapes = new HashSet<Shape>();
    }

    public Set<Shape> getAvailableShapes() {
        return availableShapes;
    }

    public void setAvailableShapes(Set<Shape> availableShapes) {
        this.availableShapes = availableShapes;
    }

    public Shape getMainShape() {
        return mainShape;
    }

    public void setMainShape(Shape mainShape) {
        this.mainShape = mainShape;
    }

    public Shape getReferencedShape() {
        return referencedShape;
    }

    public void setReferencedShape(Shape referencedShape) {
        this.referencedShape = referencedShape;
    }
}
