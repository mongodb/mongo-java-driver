/**
 * 
 */
package com.google.code.morphia.mapping.validation.classrules;

import java.util.List;
import java.util.Set;

import com.google.code.morphia.annotations.Version;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 *
 */
public class MultipleVersions implements ClassConstraint {
	
	public void check(MappedClass mc, Set<ConstraintViolation> ve) {
		List<MappedField> versionFields = mc.getFieldsAnnotatedWith(Version.class);
		if (versionFields.size() > 1)
			ve.add(new ConstraintViolation(Level.FATAL, mc, this.getClass(), "Multiple @" + Version.class
					+ " annotations are not allowed. (" + new FieldEnumString(versionFields)));
	}
}
