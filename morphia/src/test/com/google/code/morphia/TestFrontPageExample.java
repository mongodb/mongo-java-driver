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

package com.google.code.morphia;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Indexed;
import com.google.code.morphia.annotations.NotSaved;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.query.UpdateResults;

/**
 * @author Scott Hernandez
 */
@SuppressWarnings({"unchecked", "rawtypes", "unused"})
public class TestFrontPageExample  extends TestBase {

	@Entity("employees")
	private static class Employee {
		
		private Employee() {};
		public Employee(String f, String l, Key<Employee> boss, long sal) {
			firstName = f;
			lastName = l;
			manager = boss;
			salary = sal;
		}
		
		@Id ObjectId id; // auto-generated, if not set (see ObjectId)
		String firstName, lastName; // value types are automatically persisted
		Long salary = null; // only non-null values are stored 

//		Address address; // by default fields are @Embedded
		
		Key<Employee> manager; // references can be saved without automatic
								// loading
		@Reference List<Employee> underlings = new ArrayList<Employee>(); // refs are stored*, and loaded automatically
		
		@Property("started") Date startDate; // fields can be renamed
		@Property("left") Date endDate;
		
		@Indexed boolean active = false; // fields can be indexed for better performance
		@NotSaved String readButNotStored; // fields can loaded, but not saved
		@Transient int notStored; // fields can be ignored (no load/save)
		transient boolean stored = true; // not @Transient, will be ignored by Serialization/GWT for example.
	}
	
	@Test
    public void testIt() throws Exception {
		morphia.map(Employee.class);
		
		ds.save(new Employee("Mister", "GOD", null, 0));
		
		Employee boss = ds.find(Employee.class).field("manager").equal(null).get(); // get an employee without a manager
		Assert.assertNotNull(boss);
		Key<Employee> scottsKey = ds.save(new Employee("Scott", "Hernandez", ds.getKey(boss), 150*1000));
		Assert.assertNotNull(scottsKey);

		UpdateResults<Employee> res = ds.update(boss, ds.createUpdateOperations(Employee.class).add("underlings", scottsKey)); //add Scott as an employee of his manager
		Assert.assertNotNull(res);
		Assert.assertTrue(res.getUpdatedExisting());
		Assert.assertEquals(1, res.getUpdatedCount());
		
		Employee scottsBoss = ds.find(Employee.class).filter("underlings", scottsKey).get(); // get Scott's boss
		Assert.assertNotNull(scottsBoss);
		Assert.assertEquals(boss.id, scottsBoss.id);

		for (Employee e : ds.find(Employee.class, "manager", boss))
		   print(e);
	}
	
	private void print(Employee e) {};
}
