/**
 * 
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import java.util.Set;

import org.bson.types.ObjectId;

import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;
import com.google.code.morphia.utils.ReflectionUtils;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class MapKeyDifferentFromString extends FieldConstraint {
	final private static String supportedExample = "(Map<String/Enum/Long/ObjectId/..., ?>)";
	@Override
	protected void check(MappedClass mc, MappedField mf, Set<ConstraintViolation> ve) {
		if (mf.isMap() && (!mf.hasAnnotation(Serialized.class))) {
			Class<?> parameterizedClass = ReflectionUtils.getParameterizedClass(mf.getField(), 0);
			if (parameterizedClass == null) {
				ve.add(new ConstraintViolation(Level.WARNING, mc, mf, this.getClass(),
						"Maps cannot be keyed by Object (Map<Object,?>); Use a parametrized type that is supported " + supportedExample));
			} else if (!parameterizedClass.equals(String.class) && !parameterizedClass.equals(ObjectId.class) && !ReflectionUtils.isPrimitiveLike(parameterizedClass))
				ve
						.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
								"Maps must be keyed by a simple type " + supportedExample + "; " + parameterizedClass + " is not supported as a map key type."));
		}
	}
}
