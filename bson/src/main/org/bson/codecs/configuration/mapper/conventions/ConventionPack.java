/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson.codecs.configuration.mapper.conventions;

import org.bson.codecs.configuration.mapper.ClassModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines a set of Conventions to be applied when mapping classes
 *
 * @see Convention
 */
public abstract class ConventionPack {
    public static final String CLASS_MAPPING = "Class Mapping";
    public static final String FIELD_MAPPING = "Field Mapping";
    public static final String VALIDATION = "Validation";
    private final Map<String, List<Convention>> conventions = new HashMap<String, List<Convention>>();
    private final List<String> phases = new ArrayList<String>(Arrays.asList(ConventionPack.CLASS_MAPPING,
                                                                            ConventionPack.FIELD_MAPPING,
                                                                            ConventionPack.VALIDATION));

    /**
     * Creates an empty ConventionPack
     */
    public ConventionPack() {
    }

    /**
     * Creates a new ConventionPack using an existing pack as the starting point.
     *
     * @param pack the pack to base
     */
    public ConventionPack(final ConventionPack pack) {
        conventions.putAll(pack.getConventions());
    }

    /**
     * @return the map of phases and Conventions
     */
    public Map<String, List<Convention>> getConventions() {
        return conventions;
    }

    /**
     * Adds a Convention to this pack
     *
     * @param convention the Convention to add
     */
    public void addConvention(final Convention convention) {
        List<Convention> list = conventions.get(convention.getPhase());
        if (list == null) {
            list = new ArrayList<Convention>();
            conventions.put(convention.getPhase(), list);
        }
        list.add(convention);
    }

    /**
     * Applies the Conventions in the pack to the given ClassModel
     *
     * @param model the ClassModel to process
     */
    public void apply(final ClassModel model) {
        model.map();
        for (final String phase : getPhases()) {
            for (final Convention convention : getConventions(phase)) {
                convention.apply(model);
            }
        }
    }

    /**
     * Lists the phases this ConventionPack uses to process Conventions
     *
     * @return the phases of this ConventionPack
     */
    public List<String> getPhases() {
        return phases;
    }

    /**
     * Returns all the Conventions registered for a particular phase.
     *
     * @param phase the phase to list
     * @return the list of Conventions.  If the phase does not exist, this list will be empty.
     */
    public final List<Convention> getConventions(final String phase) {
        final List<Convention> list = this.conventions.get(phase);
        return list == null ? Collections.<Convention>emptyList()
                            : Collections.unmodifiableList(list);
    }

    /**
     * Determines if the class is mappable according to this ConventionPack
     *
     * @param clazz the class process
     * @return true if the pack can map this class
     */
    public abstract boolean isMappable(final Class<?> clazz);
}
