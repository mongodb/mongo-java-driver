/**
 * 
 */
package com.google.code.morphia.mapping.validation.classrules;

import java.util.Map;
import java.util.Set;

import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class EntityCannotBeMapOrIterable implements ClassConstraint {

	public void check(MappedClass mc, Set<ConstraintViolation> ve) {
		
		if (mc.getEntityAnnotation() != null && (Map.class.isAssignableFrom(mc.getClazz()) || Iterable.class.isAssignableFrom(mc.getClazz()))) {
			ve.add(new ConstraintViolation(Level.FATAL, mc, this.getClass(), "Entities cannot implement Map/Iterable"));
		}
		
	}
}
