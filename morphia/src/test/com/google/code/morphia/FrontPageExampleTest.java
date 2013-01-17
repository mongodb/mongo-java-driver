/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Indexed;
import com.google.code.morphia.annotations.NotSaved;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.query.UpdateResults;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Scott Hernandez
 */
public class FrontPageExampleTest extends TestBase {

    @Entity("employees")
    private static class Employee {

        private Employee() {
        }

        public Employee(final String f, final String l, final Key<Employee> boss, final long sal) {
            firstName = f;
            lastName = l;
            manager = boss;
            salary = sal;
        }

        @Id
        private ObjectId id; // auto-generated, if not set (see ObjectId)
        private String firstName, lastName; // value types are automatically persisted
        private Long salary = null; // only non-null values are stored

//		Address address; // by default fields are @Embedded

        private Key<Employee> manager; // references can be saved without automatic
        // loading
        @Reference
        private final List<Employee> underlings = new ArrayList<Employee>(); // refs are stored*, and loaded automatically

        @Property("started")
        private Date startDate; // fields can be renamed
        @Property("left")
        private Date endDate;

        @Indexed
        private final boolean active = false; // fields can be indexed for better performance
        @NotSaved
        private String readButNotStored; // fields can loaded, but not saved
        @Transient
        private int notStored; // fields can be ignored (no load/save)
        private final transient boolean stored = true; // not @Transient, will be ignored by Serialization/GWT for example.
    }

    @Test
    public void testIt() throws Exception {
        morphia.map(Employee.class);

        ds.save(new Employee("Mister", "GOD", null, 0));

        final Employee boss = ds.find(Employee.class).field("manager").equal(null).get(); // get an employee without
        // a manager
        Assert.assertNotNull(boss);
        final Key<Employee> scottsKey = ds.save(new Employee("Scott", "Hernandez", ds.getKey(boss), 150 * 1000));
        Assert.assertNotNull(scottsKey);

        final UpdateResults<Employee> res = ds.update(boss, ds.createUpdateOperations(Employee.class).add
                ("underlings", scottsKey)); //add Scott as an employee of his manager
        Assert.assertNotNull(res);
        Assert.assertTrue(res.getUpdatedExisting());
        Assert.assertEquals(1, res.getUpdatedCount());

        final Employee scottsBoss = ds.find(Employee.class).filter("underlings", scottsKey).get(); // get Scott's boss
        Assert.assertNotNull(scottsBoss);
        Assert.assertEquals(boss.id, scottsBoss.id);

        for (final Employee e : ds.find(Employee.class, "manager", boss)) {
            print(e);
        }
    }

    private void print(final Employee e) {
    }
}
