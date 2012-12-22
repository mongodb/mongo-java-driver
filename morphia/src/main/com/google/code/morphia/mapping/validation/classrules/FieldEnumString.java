/**
 * 
 */
package com.google.code.morphia.mapping.validation.classrules;

import java.util.Arrays;
import java.util.List;

import com.google.code.morphia.mapping.MappedField;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class FieldEnumString {
	private final String display;
	
	public FieldEnumString(MappedField... fields) {
		this(Arrays.asList(fields));
	}
	
	public FieldEnumString(List<MappedField> fields) {
		StringBuffer sb = new StringBuffer(128);
		for (MappedField mappedField : fields) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(mappedField.getNameToStore());
		}
		this.display = sb.toString();
	}
	
	@Override
	public String toString() {
		return display;
	}
}
