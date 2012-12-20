package com.google.code.morphia.mapping.validation.classrules;

import java.util.HashSet;
import java.util.Set;

import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author josephpachod
 */
public class DuplicatedAttributeNames implements ClassConstraint {
	
	public void check(MappedClass mc, Set<ConstraintViolation> ve) {
		Set<String> foundNames = new HashSet<String>();
		Set<String> duplicates = new HashSet<String>();
		for (MappedField mappedField : mc.getPersistenceFields()) {
			for(String name : mappedField.getLoadNames()) {
//				if (duplicates.contains(name)) {
//					continue;
//				}
				if (!foundNames.add(name)) {
					ve.add(new ConstraintViolation(Level.FATAL, mc, mappedField, this.getClass(), "Mapping to MongoDB field name '" + name
							+ "' is duplicated; you cannot map different java fields to the same MongoDB field."));
					duplicates.add(name);
				}
			}
		}
	}
}
