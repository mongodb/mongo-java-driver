package com.mongodb;

// TestNg

import com.mongodb.util.JSON;
import com.mongodb.util.TestCase;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public final class EnumTest extends TestCase
{
	private final DB _db;
	private final Foo _foo = new Foo(88, Colors.Blue);

	public EnumTest() throws IOException, MongoException
	{
		super();

		cleanupMongo = new Mongo("127.0.0.1");
		cleanupDB = "com_mongodb_unittest_EnumTest";
		_db = cleanupMongo.getDB(cleanupDB);
	}

	@Test
	public void JSONTest()
	{
		final String result = "{ \"Color\" : \"Blue\" , \"_id\" : 88}";
		String json = JSON.serialize(_foo);

		Assert.assertNotNull(json);

		Object obj = JSON.parse(json);

		Assert.assertNotNull(obj);
		Assert.assertEquals(json, result);
		Assert.assertEquals(obj.toString(), result);
	}

	@Test
	public void BSONTest()
	{
		byte[] b = BSON.encode(_foo);

		Assert.assertNotNull(b);

		BSONObject obj = BSON.decode(b);

		Assert.assertNotNull(obj);
		Assert.assertTrue(obj instanceof BasicBSONObject);
		Assert.assertEquals(obj.get("Color").toString(), "Blue");
	}

	@Test
	public void OnlineTest()
	{
		DBCollection c = _db.getCollection("Foo");
		c.setObjectClass(Foo.class);
		c.save(_foo);

		BasicDBObject dbRef = new BasicDBObject();
		dbRef.put("_id", 88);

		DBObject dbObject = c.findOne(dbRef);
		Assert.assertNotNull(dbObject);
		Assert.assertTrue(dbObject instanceof Foo);

		Foo foo = (Foo) dbObject;
		Assert.assertNotNull(foo);
		Assert.assertEquals(foo.get_id().toString(), "88");
		Assert.assertEquals(foo.get_color(), Colors.Blue);
	}

	public static final class Foo extends ReflectionDBObject
	{
		private Colors _color;

		public Foo()
		{
			this(0, Colors.Red);
		}

		public Foo(int id, Colors colors)
		{
			this.set_id(id);
			this.set_color(colors);
		}

		public Colors get_color()
		{
			return _color;
		}

		public void set_color(Colors colors)
		{
			this._color = colors;
		}
	}

	public enum Colors
	{
		Red(0xFF0000, "red one"),
		Green(0x00FF00, "green one"),
		Blue(0x0000FF, "blue one");

		private int _value;
		private String _description;

		private Colors(int value, String description)
		{
			this._value = value;
			this._description = description;
		}

		public int get_value()
		{
			return _value;
		}

		public String get_description()
		{
			return _description;
		}
	}
}

