/**
 * 
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import java.util.Set;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class ReferenceToUnidentifiable extends FieldConstraint {
	
	@SuppressWarnings("unchecked")
	@Override
	protected void check(MappedClass mc, MappedField mf, Set<ConstraintViolation> ve) {
		if (mf.hasAnnotation(Reference.class)) {
			Class realType = (mf.isSingleValue()) ? mf.getType() : mf.getSubClass();
			
			if (realType == null) throw new MappingException("Type is null for this MappedField: " + mf);
			
			if ((!realType.isInterface() && mc.getMapper().getMappedClass(realType).getIdField() == null))
				ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(), mf.getFullName() + " is annotated as a @"
						+ Reference.class.getSimpleName() + " but the " + mf.getType().getName()
						+ " class is missing the @" + Id.class.getSimpleName() + " annotation"));
		}
	}
	
}
