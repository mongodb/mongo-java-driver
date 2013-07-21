package com.mongodb;

import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.mongodb.util.TestCase;

public class MongoClientObjectFactoryTest extends TestCase {

	private MongoClientObjectFactory mongoClientObjectFactory;
	private Reference referenceObj;	
	private Reference misconfiguredRefObj;	
	

	@BeforeClass
	public void setup() {
		mongoClientObjectFactory = new MongoClientObjectFactory();
		referenceObj = new Reference(
				"com.mongodb.MongoClient",
				new StringRefAddr("mongoClientURI",
						"mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/jndiTest?safe=true"));
		misconfiguredRefObj  = new Reference(
				"com.mongodb.MongoClient",
				//Note the incorrect property name
				new StringRefAddr("mongoURIForClients",
						"mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/jndiTest?safe=true"));
	}

	@AfterClass
	public void tearDown() {
		mongoClientObjectFactory = null;
	}

	
	@Test
	public void testCreationUsingValidUri() throws Exception {
		Object retObj = mongoClientObjectFactory.getObjectInstance(referenceObj, null, null, null);
		Assert.assertNotNull(retObj);
		Assert.assertTrue(retObj instanceof MongoClient);
		MongoClient client = (MongoClient) retObj;		
		Assert.assertTrue(client.getAllAddress().size() == 3);
	}
	
	@Test
	public void testCreationUsingInvalidUri() throws Exception {
		try{
			mongoClientObjectFactory.getObjectInstance(misconfiguredRefObj, null, null, null);
			fail("Should fail, as the ref obj was misconfigured");
		}
		catch(MongoException mongoException){
			//All is well, and exception is welcome and expected !
		}
	}

}
