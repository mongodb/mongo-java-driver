package com.google.code.morphia.issue194;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Indexed;
import com.mongodb.MongoException;

public class IndexTest extends TestBase{
        
        static class E1 {
                @Id
                private ObjectId id;
                @Indexed(name="NAME", unique=true)
                private String name;
                
                public E1() {
                }

                public void setName(String name) {
                        this.name = name;
                }

                public String getName() {
                        return name;
                }

                public void setId(ObjectId id) {
                        this.id = id;
                }

                public ObjectId getId() {
                        return id;
                }
        }

        @Entity
        static class E2 {
                @Id
                private ObjectId id;
                @Indexed(name="NAME", unique=true)
                private String name;
                
                public E2() {
                }

                public void setName(String name) {
                        this.name = name;
                }

                public String getName() {
                        return name;
                }

                public void setId(ObjectId id) {
                        this.id = id;
                }

                public ObjectId getId() {
                        return id;
                }
        }

        @Before
        public void setUp() {
        	super.setUp();
            morphia.map(E1.class);
            morphia.map(E2.class);
            ds.ensureIndexes();
            ds.ensureCaps();
        }
        
        @Test(expected=MongoException.DuplicateKey.class)
        public void TestDuplicate1() {
                String name = "J. Doe";
                
                E1 ent11 = new E1();
                ent11.setName(name);
                ds.save(ent11);
                
                E1 ent12 = new E1();
                ent12.setName(name);
                ds.save(ent12);

        }

        @Test(expected=MongoException.DuplicateKey.class)
        public void TestDuplicate2() {
                String name = "J. Doe";

                E2 ent21 = new E2();
                ent21.setName(name);
                ds.save(ent21);
                
                E2 ent22 = new E2();
                ent22.setName(name);
                ds.save(ent22);
        }
}
