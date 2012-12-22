/**
 * 
 */
package com.google.code.morphia.mapping.validation.classrules;

import java.util.Set;

import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class NoId implements ClassConstraint {

	public void check(MappedClass mc, Set<ConstraintViolation> ve) {
		if (mc.getIdField() == null && mc.getEmbeddedAnnotation() == null) {
			ve.add(new ConstraintViolation(Level.FATAL, mc, this.getClass(),
					"No field is annotated with @Id; but it is required"));
		}
	}
	
}
