/**
 * 
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import java.util.Set;

import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.lazy.LazyFeatureDependencies;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 *
 */
public class LazyReferenceMissingDependencies extends FieldConstraint {
	
	@Override
	protected void check(MappedClass mc, MappedField mf, Set<ConstraintViolation> ve) {
		Reference ref = mf.getAnnotation(Reference.class);
		if (ref != null) {
			if (ref.lazy()) {
				if (!LazyFeatureDependencies.testDependencyFullFilled())
					ve.add(new ConstraintViolation(Level.SEVERE, mc, mf, this.getClass(),
							"Lazy references need CGLib and Proxytoys in the classpath."));
			}
		}
	}
	
}
