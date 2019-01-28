/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.codecs.pojo;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * The default Conventions
 *
 * @since 3.5
 * @see Convention
 */
public final class Conventions {

    /**
     * The default class and property conventions
     *
     * <ul>
     *     <li>Sets the discriminator key if not set to {@code _t} and the discriminator value if not set to the
     *     ClassModels simple type name.</li>
     *     <li>Configures the PropertyModels. If the {@code idProperty} isn't set and there is a
     *     property named {@code getId()}, {@code id} or {@code _id} it will be marked as the idProperty.</li>
     * </ul>
     */
    public static final Convention CLASS_AND_PROPERTY_CONVENTION = new ConventionDefaultsImpl();

    /**
     * The annotation convention.
     *
     * <p>Applies all the conventions related to the default {@link org.bson.codecs.pojo.annotations}.</p>
     */
    public static final Convention ANNOTATION_CONVENTION = new ConventionAnnotationImpl();

    /**
     * A convention that enables private fields to be set using reflection.
     *
     * <p>This convention mimics how some other JSON libraries directly set a private field when there is no setter.</p>
     * <p>Note: This convention is not part of the {@code DEFAULT_CONVENTIONS} list and must explicitly be set.</p>
     *
     * @since 3.6
     */
    public static final Convention SET_PRIVATE_FIELDS_CONVENTION = new ConventionSetPrivateFieldImpl();

    /**
     * A convention that uses getter methods as setters for collections and maps if there is no setter.
     *
     * <p>This convention mimics how JAXB mutate collections and maps.</p>
     * <p>Note: This convention is not part of the {@code DEFAULT_CONVENTIONS} list and must explicitly be set.</p>
     *
     * @since 3.6
     */
    public static final Convention USE_GETTERS_FOR_SETTERS = new ConventionUseGettersAsSettersImpl();


    /**
     * A convention that sets the IdGenerator if the id property is either a {@link org.bson.types.ObjectId} or
     * {@link org.bson.BsonObjectId}.
     *
     * @since 3.10
     */
    public static final Convention OBJECT_ID_GENERATORS = new ConventionObjectIdGeneratorsImpl();

    /**
     * The default conventions list
     */
    public static final List<Convention> DEFAULT_CONVENTIONS =
            unmodifiableList(asList(CLASS_AND_PROPERTY_CONVENTION, ANNOTATION_CONVENTION, OBJECT_ID_GENERATORS));

    /**
     * An empty conventions list
     */
    public static final List<Convention> NO_CONVENTIONS = Collections.emptyList();

    private Conventions() {
    }
}
