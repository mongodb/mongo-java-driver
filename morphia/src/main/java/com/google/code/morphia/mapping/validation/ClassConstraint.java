package com.google.code.morphia.mapping.validation;

import java.util.Set;

import com.google.code.morphia.mapping.MappedClass;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public interface ClassConstraint {
	void check(MappedClass mc, Set<ConstraintViolation> ve);
}
