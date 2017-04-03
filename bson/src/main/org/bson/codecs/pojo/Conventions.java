/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
     * The default class and field conventions
     *
     * <ul>
     *     <li>Sets the discriminator key if not set to {@code _t} and the discriminator value if not set to the
     *     ClassModels simple type name.</li>
     *     <li>Configures the FieldModels. Sets the document field name if not set to the field name.
     *     If the idField isn't set and there is a field named {@code id} or {@code _id} it will be marked as the idField.</li>
     * </ul>
     */
    public static final Convention CLASS_AND_FIELD_CONVENTION = new ConventionDefaultsImpl();

    /**
     * The annotation convention.
     *
     * <p>Applies all the conventions related to the default {@link org.bson.codecs.pojo.annotations}.</p>
     */
    public static final Convention ANNOTATION_CONVENTION = new ConventionAnnotationImpl();

    /**
     * The default conventions list
     */
    public static final List<Convention> DEFAULT_CONVENTIONS = unmodifiableList(asList(CLASS_AND_FIELD_CONVENTION, ANNOTATION_CONVENTION));

    /**
     * An empty conventions list
     */
    public static final List<Convention> NO_CONVENTIONS = Collections.emptyList();


    private Conventions() {
    }
}
