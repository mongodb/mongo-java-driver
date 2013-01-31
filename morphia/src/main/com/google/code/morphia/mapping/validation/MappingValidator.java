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

package com.google.code.morphia.mapping.validation;

/**
 *
 */

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;
import com.google.code.morphia.mapping.validation.classrules.DuplicatedAttributeNames;
import com.google.code.morphia.mapping.validation.classrules.EmbeddedAndId;
import com.google.code.morphia.mapping.validation.classrules.EmbeddedAndValue;
import com.google.code.morphia.mapping.validation.classrules.EntityAndEmbed;
import com.google.code.morphia.mapping.validation.classrules.EntityCannotBeMapOrIterable;
import com.google.code.morphia.mapping.validation.classrules.MultipleId;
import com.google.code.morphia.mapping.validation.classrules.MultipleVersions;
import com.google.code.morphia.mapping.validation.classrules.NoId;
import com.google.code.morphia.mapping.validation.fieldrules.ContradictingFieldAnnotation;
import com.google.code.morphia.mapping.validation.fieldrules.LazyReferenceMissingDependencies;
import com.google.code.morphia.mapping.validation.fieldrules.LazyReferenceOnArray;
import com.google.code.morphia.mapping.validation.fieldrules.MapKeyDifferentFromString;
import com.google.code.morphia.mapping.validation.fieldrules.MapNotSerializable;
import com.google.code.morphia.mapping.validation.fieldrules.MisplacedProperty;
import com.google.code.morphia.mapping.validation.fieldrules.ReferenceToUnidentifiable;
import com.google.code.morphia.mapping.validation.fieldrules.VersionMisuse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class MappingValidator {

    private static final Logr logger = MorphiaLoggerFactory.get(MappingValidator.class);

    public void validate(final List<MappedClass> classes) {
        final Set<ConstraintViolation> ve = new TreeSet<ConstraintViolation>(new Comparator<ConstraintViolation>() {

            public int compare(final ConstraintViolation o1, final ConstraintViolation o2) {
                return o1.getLevel().ordinal() > o2.getLevel().ordinal() ? -1 : 1;
            }
        });

        final List<ClassConstraint> rules = getConstraints();
        for (final MappedClass c : classes) {
            for (final ClassConstraint v : rules) {
                v.check(c, ve);
            }
        }

        if (!ve.isEmpty()) {
            final ConstraintViolation worst = ve.iterator().next();
            final Level maxLevel = worst.getLevel();
            if (maxLevel.ordinal() >= Level.FATAL.ordinal()) {
                throw new ConstraintViolationException(ve);
            }

            // sort by class to make it more readable
            final ArrayList<LogLine> l = new ArrayList<LogLine>();
            for (final ConstraintViolation v : ve) {
                l.add(new LogLine(v));
            }
            Collections.sort(l);

            for (final LogLine line : l) {
                line.log(MappingValidator.logger);
            }
        }
    }

    private List<ClassConstraint> getConstraints() {
        final List<ClassConstraint> constraints = new ArrayList<ClassConstraint>(32);

        // normally, i do this with scanning the classpath, but that´d bring
        // another dependency ;)

        // class-level
        constraints.add(new MultipleId());
        constraints.add(new MultipleVersions());
        constraints.add(new NoId());
        constraints.add(new EmbeddedAndId());
        constraints.add(new EntityAndEmbed());
        constraints.add(new EmbeddedAndValue());
        constraints.add(new EntityCannotBeMapOrIterable());
        constraints.add(new DuplicatedAttributeNames());
        //		constraints.add(new ContainsEmbeddedWithId());
        // field-level
        constraints.add(new MisplacedProperty());
        constraints.add(new ReferenceToUnidentifiable());
        constraints.add(new LazyReferenceMissingDependencies());
        constraints.add(new LazyReferenceOnArray());
        constraints.add(new MapKeyDifferentFromString());
        constraints.add(new MapNotSerializable());
        constraints.add(new VersionMisuse());
        //
        constraints.add(new ContradictingFieldAnnotation(Reference.class, Serialized.class));
        constraints.add(new ContradictingFieldAnnotation(Reference.class, Property.class));
        constraints.add(new ContradictingFieldAnnotation(Reference.class, Embedded.class));
        //
        constraints.add(new ContradictingFieldAnnotation(Embedded.class, Serialized.class));
        constraints.add(new ContradictingFieldAnnotation(Embedded.class, Property.class));
        //
        constraints.add(new ContradictingFieldAnnotation(Property.class, Serialized.class));

        return constraints;
    }

    class LogLine implements Comparable<LogLine> {
        private final ConstraintViolation v;

        LogLine(final ConstraintViolation v) {
            this.v = v;
        }

        void log(final Logr logger) {
            switch (v.getLevel()) {
                case SEVERE:
                    logger.error(v.render());
                    break;
                case WARNING:
                    logger.warning(v.render());
                    break;
                case INFO:
                    logger.info(v.render());
                    break;
                case MINOR:
                    logger.debug(v.render());
                    break;

                default:
                    throw new IllegalStateException("Cannot log " + ConstraintViolation.class.getSimpleName()
                                                    + " of Level " + v.getLevel());
            }
        }

        public int compareTo(final LogLine o) {
            return v.getPrefix().compareTo(o.v.getPrefix());
        }
    }

    /**
     * i definitely vote for all at once validation
     */
    @Deprecated
    public void validate(final MappedClass mappedClass) {
        validate(Arrays.asList(mappedClass));
    }
}
