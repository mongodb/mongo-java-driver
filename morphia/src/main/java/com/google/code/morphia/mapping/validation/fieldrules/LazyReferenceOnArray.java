/**
 * 
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import java.util.Set;

import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
@SuppressWarnings("unchecked")
public class LazyReferenceOnArray extends FieldConstraint {
	
	@Override
	protected void check(MappedClass mc, MappedField mf, Set<ConstraintViolation> ve) {
		Reference ref = mf.getAnnotation(Reference.class);
		if (ref != null && ref.lazy()) {
			Class type = mf.getType();
			if (type.isArray())
				ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
						"The lazy attribute cannot be used for an Array. If you need a lazy array please use ArrayList instead."));
		}
	}
	
}
