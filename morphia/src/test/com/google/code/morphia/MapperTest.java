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
import com.google.code.morphia.annotations.PostLoad;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.lazy.LazyFeatureDependencies;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

/**
 * Tests mapper functions; this is tied to some of the internals.
 *
 * @author scotthernandez
 */
public class MapperTest extends TestBase {
    public static class A {
        private static int loadCount = 0;
        @Id
        private ObjectId id;

        String getId() {
            return id.toString();
        }

        @PostLoad
        protected void postConstruct() {
            if (A.loadCount > 1) {
                throw new RuntimeException();
            }

            A.loadCount++;
        }
    }

    @Entity("holders")
    static class HoldsMultipleA {
        @Id
        private ObjectId id;
        @Reference
        private A a1;
        @Reference
        private A a2;
    }

    @Entity("holders")
    static class HoldsMultipleALazily {
        @Id
        private ObjectId id;
        @Reference(lazy = true)
        private A a1;
        @Reference
        private A a2;
        @Reference(lazy = true)
        private A a3;
    }

    static class CustomId implements Serializable {

        private static final long serialVersionUID = 1L;

        @Property("v")
        ObjectId id;
        @Property("t")
        String type;

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof CustomId)) {
                return false;
            }
            final CustomId other = (CustomId) obj;
            if (id == null) {
                if (other.id != null) {
                    return false;
                }
            }
            else if (!id.equals(other.id)) {
                return false;
            }
            if (type == null) {
                if (other.type != null) {
                    return false;
                }
            }
            else if (!type.equals(other.type)) {
                return false;
            }
            return true;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("CustomId [");
            if (id != null) {
                builder.append("id=").append(id).append(", ");
            }
            if (type != null) {
                builder.append("type=").append(type);
            }
            builder.append("]");
            return builder.toString();
        }
    }

    static class UsesCustomIdObject {
        @Id
        CustomId id;
        String text;
    }

    @Test
    public void singleLookup() {
        A.loadCount = 0;
        final A a = new A();
        HoldsMultipleA holder = new HoldsMultipleA();
        holder.a1 = a;
        holder.a2 = a;
        ds.save(a, holder);
        holder = ds.get(HoldsMultipleA.class, holder.id);
        Assert.assertEquals(1, A.loadCount);
        Assert.assertTrue(holder.a1 == holder.a2);
    }

    @Test
    public void singleProxy() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        A.loadCount = 0;
        final A a = new A();
        HoldsMultipleALazily holder = new HoldsMultipleALazily();
        holder.a1 = a;
        holder.a2 = a;
        holder.a3 = a;
        ds.save(a, holder);
        Assert.assertEquals(0, A.loadCount);
        holder = ds.get(HoldsMultipleALazily.class, holder.id);
        Assert.assertNotNull(holder.a2);
        Assert.assertEquals(1, A.loadCount);
        Assert.assertFalse(holder.a1 == holder.a2);
        // FIXME currently not guaranteed:
        // Assert.assertTrue(holder.a1 == holder.a3);

        // A.loadCount=0;
        // Assert.assertEquals(holder.a1.getId(), holder.a2.getId());

    }

    @Test
    public void serializableId() {
        final CustomId cId = new CustomId();
        cId.id = new ObjectId();
        cId.type = "banker";

        final UsesCustomIdObject ucio = new UsesCustomIdObject();
        ucio.id = cId;
        ucio.text = "hllo";
        ds.save(ucio);
    }

}
