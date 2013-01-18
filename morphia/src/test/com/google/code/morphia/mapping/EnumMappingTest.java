/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PreSave;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class EnumMappingTest extends TestBase {
    static class ContainsEnum {
        @Id
        private ObjectId id;
        private final Foo foo = Foo.BAR;

        @PreSave
        void testMapping() {

        }
    }

    static enum Foo {
        BAR() {
        },
        BAZ
    }

    @Test
    public void testEnumMapping() throws Exception {
        morphia.map(ContainsEnum.class);

        ds.save(new ContainsEnum());
        Assert.assertEquals(1, ds.createQuery(ContainsEnum.class).field("foo").equal(Foo.BAR).countAll());
        Assert.assertEquals(1, ds.createQuery(ContainsEnum.class).filter("foo", Foo.BAR).countAll());
        Assert.assertEquals(1, ds.createQuery(ContainsEnum.class).disableValidation().filter("foo",
                                                                                             Foo.BAR).countAll());
    }


}
