package com.google.code.morphia.mapping.validation;

/**
 * 
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class MappingValidator {
	
	private static final Logr logger = MorphiaLoggerFactory.get(MappingValidator.class);
	
	public void validate(List<MappedClass> classes) {
		Set<ConstraintViolation> ve = new TreeSet<ConstraintViolation>(new Comparator<ConstraintViolation>() {
			
			public int compare(ConstraintViolation o1, ConstraintViolation o2) {
				return o1.getLevel().ordinal() > o2.getLevel().ordinal() ? -1 : 1;
			}
		});

		List<ClassConstraint> rules = getConstraints();
		for (MappedClass c : classes) {
			for (ClassConstraint v : rules) {
				v.check(c, ve);
			}
		}

		if (!ve.isEmpty()) {
			ConstraintViolation worst = ve.iterator().next();
			Level maxLevel = worst.getLevel();
			if (maxLevel.ordinal() >= Level.FATAL.ordinal()) {
				throw new ConstraintViolationException(ve);
			}
			
			// sort by class to make it more readable
			ArrayList<LogLine> l = new ArrayList<LogLine>();
			for (ConstraintViolation v : ve) {
				l.add(new LogLine(v));
			}
			Collections.sort(l);

			for (LogLine line : l) {
				line.log(MappingValidator.logger);
			}
		}
	}
	
	private List<ClassConstraint> getConstraints() {
		List<ClassConstraint> constraints = new ArrayList<ClassConstraint>(32);
		
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
		private ConstraintViolation v;

		LogLine(ConstraintViolation v) {
			this.v = v;
		}
		
		void log(Logr logger) {
			switch (v.getLevel()) {
				case SEVERE:
					logger.error(v.render());
				case WARNING:
					logger.warning(v.render());
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
		
		public int compareTo(LogLine o) {
			return v.getPrefix().compareTo(o.v.getPrefix());
		}
	}
	
	/**
	 * i definitely vote for all at once validation
	 */
	@Deprecated
	public void validate(MappedClass mappedClass) {
		validate(Arrays.asList(mappedClass));
	}
}
