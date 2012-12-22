/**
 * 
 */
package com.google.code.morphia.mapping.validation.classrules;

import java.util.Set;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class EntityAndEmbed implements ClassConstraint {

	public void check(MappedClass mc, Set<ConstraintViolation> ve) {
		
		if (mc.getEntityAnnotation() != null && mc.getEmbeddedAnnotation() != null) {
			new ConstraintViolation(Level.FATAL, mc, this.getClass(), "Cannot have both @" + Entity.class.getSimpleName() + " and @"
					+ Embedded.class.getSimpleName() + " annotation at class level.");
		}
		
	}
}
