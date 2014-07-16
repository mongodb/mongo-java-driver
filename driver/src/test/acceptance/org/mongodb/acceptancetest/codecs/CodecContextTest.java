package org.mongodb.acceptancetest.codecs;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.MongoCollection;
import org.mongodb.codecs.BsonTypeClassMap;
import org.mongodb.codecs.CollectibleCodec;
import org.mongodb.codecs.DocumentCodecProvider;
import org.mongodb.codecs.ListCodec;

public class CodecContextTest extends DatabaseTestCase {

	@Test
	public void codecContextTest() {
		MongoCollection col = database.getCollection("codecContextTest", new CustomCodec());

		List<TestObject> list = Arrays.asList(TOHandler.createInstance().setValue(3),
				TOHandler.createInstance().setValue(2).setInner(TOHandler.createInstance().setValue(3)));

		col.save(TOHandler.createInstance().setValue(3).setInner(
				TOHandler.createInstance().setValue(3).setInner(TOHandler.createInstance().setValue(2).setInners(list))));
		System.out.println("test");
	}

	private static class CustomCodec implements CollectibleCodec<TestObject> {
		CodecRegistry registry = new RootCodecRegistry(Arrays.asList(new TestCodecProvider(),
				new DocumentCodecProvider()));

		@Override
		public void encode(BsonWriter writer, TestObject value, EncoderContext encoderContext) {
			boolean asString = encoderContext.getParameter("asString") != null
					&& (Boolean) encoderContext.getParameter("asString") == true;
			EncoderContext newContext = EncoderContext.builder().addParameter("asString", !asString).build();
			// lets alternate between settings
			writer.writeStartDocument();

			if (value == null) {
				writer.writeEndDocument();
				return;
			}

			if (asString) {
				// toggle encoderContext

				writer.writeName("value");
				switch (value.getValue()) {
				case 3:
					writer.writeString("drei");
					break;
				case 2:
					writer.writeString("zwo");
					break;
				default:
					writer.writeString("N/A");
				}
			} else {
				writer.writeInt32("value", value.getValue());
			}

			if (value.getInners() != null && value.getInners().size() != 0) {
				writer.writeName("array");
				Codec codec = registry.get(List.class);
				codec.encode(writer, value.getInners(), newContext);
			}

			if (value.getInner() != null) {
				writer.writeName("inner");
				encode(writer, value.getInner(), newContext);
			}
			writer.writeEndDocument();
		}

		@Override
		public Class getEncoderClass() {
			return TestObject.class;
		}

		@Override
		public TestObject decode(BsonReader reader, DecoderContext decoderContext) {
			return null;
		}

		@Override
		public void generateIdIfAbsentFromDocument(TestObject document) {

		}

		@Override
		public boolean documentHasId(TestObject document) {
			return false;
		}

		@Override
		public BsonValue getDocumentId(TestObject document) {
			return null;
		}
	}

	private static interface TestObject {
		public int getValue();

		public TestObject setValue(int o);

		public TestObject getInner();

		public TestObject setInner(TestObject inner);

		public List<TestObject> getInners();

		public TestObject setInners(List<TestObject> inners);
	}

	private static class TOHandler implements InvocationHandler {

		private Object value;

		private TestObject inner;

		private List<TestObject> inners = new ArrayList();

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if ("setValue".equals(method.getName())) {
				value = args[0];
				return proxy;
			} else if ("getValue".equals(method.getName())) {
				return value;
			}
			if ("setInner".equals(method.getName())) {
				inner = (TestObject) args[0];
				return proxy;
			} else if ("getInner".equals(method.getName())) {
				return inner;
			}
			if ("setInners".equals(method.getName())) {
				inners = (List<TestObject>) args[0];
				return proxy;
			} else if ("getInners".equals(method.getName())) {
				return inners;
			}
			return null;
		}

		static TestObject createInstance() {
			return (TestObject) Proxy.newProxyInstance(TOHandler.class.getClassLoader(),
					new Class<?>[] { TestObject.class }, new TOHandler());
		}

	}

	private static class TestCodecProvider implements CodecProvider {

		private Map<BsonType, Class<?>> replacement;

		public TestCodecProvider() {
			replacement = new HashMap<BsonType, Class<?>>();
			replacement.put(BsonType.DOCUMENT, TestObject.class);
		}

		@Override
		public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
			if (TestObject.class.isAssignableFrom(clazz)) {
				return (Codec<T>) new CustomCodec();
			}
			if (List.class.isAssignableFrom(clazz)) {
				return (Codec<T>) new ListCodec(registry, new BsonTypeClassMap(replacement));
			}
			return null;
		}
	}
}
