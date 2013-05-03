package org.mongodb.codecs;

import org.bson.BSONBinaryReader;
import org.bson.BSONWriter;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.DBRef;

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded;

public class DBRefCodecTest {
    //CHECKSTYLE:OFF
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;

    private DBRefCodec dbRefCodec;
    private Codecs codecs;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        bsonWriter = context.mock(BSONWriter.class);
        codecs = context.mock(Codecs.class);
        dbRefCodec = new DBRefCodec(codecs);
    }

    @Test
    public void shouldEncodeDbRefAsStringNamespaceAndDelegateEncodingOfIdToCodecs() {
        final String namespace = "theNamespace";
        final String theId = "TheId";
        final DBRef dbRef = new DBRef(theId, namespace);
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeString("$ref", namespace);
            oneOf(bsonWriter).writeName("$id");
            oneOf(codecs).encode(bsonWriter, theId);
            oneOf(bsonWriter).writeEndDocument();
        }});

        dbRefCodec.encode(bsonWriter, dbRef);
    }

    @Test
    @Ignore("decoding not implemented yet")
    public void shouldDecodeCodeWithScope() throws Exception {
        final String namespace = "theNamespace";
        final String theId = "TheId";
        final DBRef dbRef = new DBRef(theId, namespace);
        final BSONBinaryReader reader = prepareReaderWithObjectToBeDecoded(dbRef);

//        final DBRef actualDBRef = dbRefCodec.decode(reader);

//        assertThat(actualDBRef, is(dbRef));
    }

}
