/**
 * 
 */
package com.google.code.morphia.callbacks;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.AbstractEntityInterceptor;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.callbacks.TestSimpleValidationViaInterceptor.NonNullValidation.NonNullValidationException;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class TestSimpleValidationViaInterceptor extends TestBase {
	
	static class E {
		@Id
		private ObjectId _id = new ObjectId();
		
		@NonNull
		Date lastModified;
		
		@PrePersist
		void entityCallback() {
			lastModified = new Date();
		}
	}
	
	static class E2 {
		@Id
		private ObjectId _id = new ObjectId();
		
		@NonNull
		String mustFailValidation;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target( { ElementType.FIELD })
	public static @interface NonNull {
	}
	
	public static class NonNullValidation extends AbstractEntityInterceptor {
		public void prePersist(Object ent, DBObject dbObj, Mapper mapr) {
			MappedClass mc = mapr.getMappedClass(ent);
			List<MappedField> fieldsToTest = mc.getFieldsAnnotatedWith(NonNull.class);
			for (MappedField mf : fieldsToTest) {
				if (mf.getFieldValue(ent) == null)
					throw new NonNullValidationException(mf);
			}
		}
		
		static class NonNullValidationException extends RuntimeException {
			
			public NonNullValidationException(MappedField mf) {
				super("NonNull field is null " + mf.getFullName());
			}

		}
	}
	
	static {
		MappedField.interestingAnnotations.add(NonNull.class);
	}

	@Test
	public void testGlobalEntityInterceptorWorksAfterEntityCallback() {

		morphia.getMapper().addInterceptor(new NonNullValidation());
		morphia.map(E.class);
		morphia.map(E2.class);
		
		ds.save(new E());
		try {
			ds.save(new E2());
			Assert.fail();
		} catch (NonNullValidationException e) {
			// expected
		}
		
	}
}
