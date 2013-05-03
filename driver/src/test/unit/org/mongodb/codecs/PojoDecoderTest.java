package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.types.CodeWithScope;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.codecs.pojo.Address;
import org.mongodb.codecs.pojo.AllPrimitiveTypes;
import org.mongodb.codecs.pojo.ListPojo;
import org.mongodb.codecs.pojo.MapPojo;
import org.mongodb.codecs.pojo.Name;
import org.mongodb.codecs.pojo.Person;
import org.mongodb.codecs.pojo.SetPojo;
import org.mongodb.codecs.pojo.SingleFieldPojo;

import java.util.ArrayList;
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

    private Codecs codecs = Codecs.createDefault();
    private PojoDecoder pojoDecoder;

    @Before
    public void setUp() throws Exception {
        pojoDecoder = new PojoDecoder(codecs);
    }

    @Test
    public void shouldDecodeComplexType() throws Exception {
        final Person person = new Person(new Address(), new Name());
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(person, new PojoCodec<Person>(codecs, Person.class));

        final Person decodedObject = pojoDecoder.decode(reader, Person.class);

        assertThat(decodedObject, is(person));
    }

    @Test
    public void shouldDecodeSimplePojoContainingSingleObject() throws Exception {
        // given
        final SingleFieldPojo singleValuePojo = new SingleFieldPojo("A Value");
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(singleValuePojo,
                                                                     new PojoCodec<SingleFieldPojo>(codecs, SingleFieldPojo.class));

        // when
        final SingleFieldPojo decodedPojo = pojoDecoder.decode(reader, SingleFieldPojo.class);

        // then
        assertThat(decodedPojo, is(singleValuePojo));
    }

    @Test
    public void shouldDecodePojoWithBsonPrimitiveFields() throws Exception {
        // given
        final AllPrimitiveTypes pojo = new AllPrimitiveTypes();
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<AllPrimitiveTypes>(codecs, AllPrimitiveTypes.class));

        // when
        final AllPrimitiveTypes decodedPojo = pojoDecoder.decode(reader, AllPrimitiveTypes.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePattern() throws Exception {
        //Pattern doesn't implement equals, so we have to jump through some hoops
        final Pattern expectedDecodedPattern = Pattern.compile("^hello");
        final SingleFieldPojo pojo = new SingleFieldPojo(expectedDecodedPattern);

        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<SingleFieldPojo>(codecs, SingleFieldPojo.class));

        // when
        final SingleFieldPojo decodedPojo = pojoDecoder.decode(reader, SingleFieldPojo.class);

        // then
        final Pattern actualDecodedPattern = (Pattern) decodedPojo.getField();
        assertThat(actualDecodedPattern.pattern(), is(expectedDecodedPattern.pattern()));
    }

    @Test
    public void shouldDecodeCodeWithScope() throws Exception {
        final SingleFieldPojo pojo = new SingleFieldPojo(new CodeWithScope("javaScript code",
                                                                           new Document("fieldNameOfScope", "valueOfScope")));

        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<SingleFieldPojo>(codecs, SingleFieldPojo.class));

        // when
        final SingleFieldPojo decodedPojo = pojoDecoder.decode(reader, SingleFieldPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePojoWithMapContainingPojos() {
        final Map<String, Person> map = new HashMap<String, Person>();
        map.put("person", new Person(new Address(), new Name()));
        final MapPojo pojo = new MapPojo(map);

        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<MapPojo>(codecs, MapPojo.class));

        // when
        final MapPojo decodedPojo = pojoDecoder.decode(reader, MapPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePojoWithListContainingPojos() {
        final List<Person> list = new ArrayList<Person>();
        list.add(new Person(new Address(), new Name()));
        final ListPojo pojo = new ListPojo(list);

        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<ListPojo>(codecs, ListPojo.class));

        // when
        final ListPojo decodedPojo = pojoDecoder.decode(reader, ListPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    @Test
    public void shouldDecodePojoWithSetContainingPojos() {
        final Set<Person> set = new HashSet<Person>();
        set.add(new Person(new Address(), new Name()));
        final SetPojo pojo = new SetPojo(set);

        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<SetPojo>(codecs, SetPojo.class));

        // when
        final SetPojo decodedPojo = pojoDecoder.decode(reader, SetPojo.class);

        // then
        assertThat(decodedPojo, is(pojo));
    }

    // TODO: not sure if it should throw an exception or ignore it but log it
    @Test(expected = DecodingException.class)
    public void shouldThrowAnExceptionIfTheFieldIsNotPartOfThePojo() {
        final IncorrectFieldPojo pojo = new IncorrectFieldPojo();

        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<IncorrectFieldPojo>(codecs, IncorrectFieldPojo.class));

        pojoDecoder.decode(reader, SingleFieldPojo.class);
    }

    static class IncorrectFieldPojo {
        @SuppressWarnings("UnusedDeclaration")
        private Object incorrectFieldName = "Some value";
    }

    @Test(expected = DecodingException.class)
    public void shouldNotBeAbleToDecodeAPojoThatIsPrivate() {
        final PrivateClass pojo = new PrivateClass();
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
                                                                     new PojoCodec<PrivateClass>(codecs,
                                                                                                 PrivateClass.class));
        pojoDecoder.decode(reader, PrivateClass.class);
    }

    private static class PrivateClass { }

    @Test(expected = DecodingException.class)
    public void shouldNotBeAbleToDecodeAPojoWithoutANoArgsConstructor() {
        final WithoutNoArgsConstructor pojo = new WithoutNoArgsConstructor(1);
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(pojo,
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
