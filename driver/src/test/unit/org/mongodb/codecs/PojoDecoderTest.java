/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.types.CodeWithScope;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.codecs.pojo.Address;
import org.mongodb.codecs.pojo.AllPrimitiveTypes;
import org.mongodb.codecs.pojo.ArrayWrapper;
import org.mongodb.codecs.pojo.ListPojo;
import org.mongodb.codecs.pojo.ListWrapper;
import org.mongodb.codecs.pojo.MapPojo;
import org.mongodb.codecs.pojo.MapWrapper;
import org.mongodb.codecs.pojo.Name;
import org.mongodb.codecs.pojo.Person;
import org.mongodb.codecs.pojo.SetPojo;
import org.mongodb.codecs.pojo.SingleFieldPojo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded;

public class PojoDecoderTest {

    private final EncoderRegistry encoderRegistry = new EncoderRegistry();
    private final Codecs codecs = new Codecs(PrimitiveCodecs.createDefault(), encoderRegistry);
    private PojoDecoder pojoDecoder;

    @Before
    public void setUp() {
        pojoDecoder = new PojoDecoder(codecs);
    }

    @Test
    public void shouldDecodeComplexType() {
        Person person = new Person(new Address(), new Name());
        BSONReader reader = prepareReaderWithObjectToBeDecoded(person, new PojoCodec<Person>(codecs, Person.class));

        Person decodedObject = pojoDecoder.decode(reader, Person.class);

        assertThat(decodedObject, is(person));
    }

    @Test
    public void shouldDecodeSimplePojoContainingSingleObject() {
        // given
        SingleFieldPojo singleValuePojo = new SingleFieldPojo("A Value");
        BSONReader reader = prepareReaderWithObjectToBeDecoded(singleValuePojo,
                                                               new PojoCodec<SingleFieldPojo>(codecs, SingleFieldPojo.class));

        // when
        SingleFieldPojo decodedPojo = pojoDecoder.decode(reader, SingleFieldPojo.class);

        // then
        assertThat(decodedPojo, is(singleValuePojo));
    }

    @Test
    public void shouldDecodePojoWithBsonPrimitiveFields() {
        // given
        AllPrimitiveTypes pojo = new AllPrimitiveTypes();
        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<AllPrimitiveTypes>(codecs, AllPrimitiveTypes.class));

        // when
        AllPrimitiveTypes decodedPojo = pojoDecoder.decode(reader, AllPrimitiveTypes.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePattern() {
        //Pattern doesn't implement equals, so we have to jump through some hoops
        Pattern expectedDecodedPattern = Pattern.compile("^hello");
        SingleFieldPojo pojo = new SingleFieldPojo(expectedDecodedPattern);

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<SingleFieldPojo>(codecs, SingleFieldPojo.class));

        // when
        SingleFieldPojo decodedPojo = pojoDecoder.decode(reader, SingleFieldPojo.class);

        // then
        Pattern actualDecodedPattern = (Pattern) decodedPojo.getField();
        assertThat(actualDecodedPattern.pattern(), is(expectedDecodedPattern.pattern()));
    }

    @Test
    public void shouldDecodeCodeWithScope() {
        SingleFieldPojo pojo = new SingleFieldPojo(new CodeWithScope("javaScript code",
                                                                     new Document("fieldNameOfScope", "valueOfScope")));

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                               new PojoCodec<SingleFieldPojo>(codecs, SingleFieldPojo.class));

        // when
        SingleFieldPojo decodedPojo = pojoDecoder.decode(reader, SingleFieldPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePojoWithMapContainingPojos() {
        encoderRegistry.register(Person.class, new PojoCodec<Person>(codecs, Person.class));
        Map<String, Person> map = new HashMap<String, Person>();
        map.put("person", new Person(new Address(), new Name()));
        MapPojo pojo = new MapPojo(map);

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<MapPojo>(codecs, MapPojo.class));

        // when
        MapPojo decodedPojo = pojoDecoder.decode(reader, MapPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePojoWithMapContainingPrimitives() {
        MapWrapper pojo = new MapWrapper();
        Map<String, String> map = new HashMap<String, String>();
        map.put("field1", "field 1 value");
        pojo.setTheMap(map);

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<MapWrapper>(codecs, MapWrapper.class));

        // when
        MapWrapper decodedPojo = pojoDecoder.decode(reader, MapWrapper.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePojoWithListContainingPojos() {
        encoderRegistry.register(Person.class, new PojoCodec<Person>(codecs, Person.class));
        List<Person> list = new ArrayList<Person>();
        list.add(new Person(new Address(), new Name()));
        ListPojo pojo = new ListPojo(list);

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<ListPojo>(codecs, ListPojo.class));

        // when
        ListPojo decodedPojo = pojoDecoder.decode(reader, ListPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePojoWithListContainingPrimitives() {
        ListWrapper pojo = new ListWrapper(Arrays.asList(1, 2, 3));

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<ListWrapper>(codecs, ListWrapper.class));

        // when
        ListWrapper decodedPojo = pojoDecoder.decode(reader, ListWrapper.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test(expected = DecodingException.class)
    public void shouldNotDecodePojoWithArray() {
        ArrayWrapper pojo = new ArrayWrapper(new int[]{1, 2, 3});

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<ArrayWrapper>(codecs, ArrayWrapper.class));

        // exception thrown here
        pojoDecoder.decode(reader, ArrayWrapper.class);
    }

    @Test
    public void shouldDecodePojoWithSetContainingPojos() {
        encoderRegistry.register(Person.class, new PojoCodec<Person>(codecs, Person.class));
        Set<Person> set = new HashSet<Person>();
        set.add(new Person(new Address(), new Name()));
        SetPojo pojo = new SetPojo(set);

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<SetPojo>(codecs, SetPojo.class));

        // when
        SetPojo decodedPojo = pojoDecoder.decode(reader, SetPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    // TODO: not sure if it should throw an exception or ignore it but log it
    @Test(expected = DecodingException.class)
    public void shouldThrowAnExceptionIfTheFieldIsNotPartOfThePojo() {
        IncorrectFieldPojo pojo = new IncorrectFieldPojo();

        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<IncorrectFieldPojo>(codecs, IncorrectFieldPojo.class));

        pojoDecoder.decode(reader, SingleFieldPojo.class);
    }

    static class IncorrectFieldPojo {
        @SuppressWarnings("UnusedDeclaration")
        private final Object incorrectFieldName = "Some value";
    }

    @Test(expected = DecodingException.class)
    public void shouldNotBeAbleToDecodeAPojoThatIsPrivate() {
        PrivateClass pojo = new PrivateClass();
        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo, new PojoCodec<PrivateClass>(codecs,
                                                                                                 PrivateClass.class));
        pojoDecoder.decode(reader, PrivateClass.class);
    }

    private static class PrivateClass { }

    @Test(expected = DecodingException.class)
    public void shouldNotBeAbleToDecodeAPojoWithoutANoArgsConstructor() {
        WithoutNoArgsConstructor pojo = new WithoutNoArgsConstructor(1);
        BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                               new PojoCodec<WithoutNoArgsConstructor>(codecs,
                                                                                                       WithoutNoArgsConstructor.class));
        pojoDecoder.decode(reader, WithoutNoArgsConstructor.class);
    }

    static class WithoutNoArgsConstructor {
        @SuppressWarnings("UnusedParameters")
        WithoutNoArgsConstructor(final int someArg) { }
    }

    @Ignore("Not implemented.  Not sure how to at this stage, or if it's necessary")
    @Test
    public void shouldBeAbleToInstantiateWithDifferentConstructors() {
        ///or provide a factory if there's no default constructor
        fail("Not implemented");
    }
}
