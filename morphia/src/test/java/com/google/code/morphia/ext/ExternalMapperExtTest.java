/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia.ext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.Key;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;

/**
 *
 * @author Scott Hernandez
 */
public class ExternalMapperExtTest extends TestBase {
	
	/**
	 * The skeleton to apply from.
	 * @author skot
	 *
	 */
	@Entity("special")
	private static class Skeleton {
		@Id String id;
	}
	
	/**
	 * Uneditable class
	 * @author skot
	 *
	 */
	private static class EntityWithNoAnnotations {
		String id;
	}
	
	private static class CloneMapper {
		Mapper mapr;
		public CloneMapper(Mapper mapr) {
			this.mapr = mapr;
		}
		
		void map(Class sourceClass, Class destClass){
			MappedClass destMC = mapr.getMappedClass(destClass);
			MappedClass sourceMC = mapr.getMappedClass(sourceClass);
			//copy the class level annotations
			for(Entry<Class<? extends Annotation>, ArrayList<Annotation>> e: sourceMC.getReleventAnnotations().entrySet()) {
				if ( e.getValue() != null && e.getValue().size() > 0)
					for(Annotation ann : e.getValue())
						destMC.addAnnotation(e.getKey(), ann);
			}
			//copy the fields.
			for(MappedField mf : sourceMC.getPersistenceFields()){
				Map<Class<? extends Annotation>, Annotation> annMap = mf.getAnnotations();
				MappedField destMF = destMC.getMappedFieldByJavaField(mf.getJavaFieldName());
				if (destMF != null && annMap != null && annMap.size() > 0) {
					for(Entry e : annMap.entrySet())
						destMF.addAnnotation((Class)e.getKey(), (Annotation)e.getValue());
				}
			}
			
		}
		
		MappedClass addAnnotation(Class clazz, String field, Annotation... annotations) {
			if (annotations == null || annotations.length == 0)
				throw new IllegalArgumentException("Must specify annotations");
			
			MappedClass mc = mapr.getMappedClass(clazz);
			MappedField mf = mc.getMappedFieldByJavaField(field);
			if(mf == null)
				throw new IllegalArgumentException("Field \""+ field + "\" does not exist on: " + mc);
			
			for(Annotation an : annotations)
				mf.putAnnotation(an);
			
			return mc;}
			
		}
		
	@Test
	public void testExternalMapping() throws Exception {
		Mapper mapr = morphia.getMapper();
		ExternalMapperExtTest.CloneMapper helper = new CloneMapper(mapr);
		helper.map(Skeleton.class, EntityWithNoAnnotations.class);
		MappedClass mc = mapr.getMappedClass(EntityWithNoAnnotations.class); 
		mc.update();
		assertNotNull(mc.getIdField());
		assertNotNull(mc.getEntityAnnotation());
		Assert.assertEquals("special", mc.getEntityAnnotation().value());
		
		EntityWithNoAnnotations ent = new EntityWithNoAnnotations();
		ent.id = "test";
		Key<EntityWithNoAnnotations> k = ds.save(ent);
		assertNotNull(k);
		ent = ds.get(EntityWithNoAnnotations.class, "test");
		assertNotNull(ent);
		assertEquals("test", ent.id);
	}
}

