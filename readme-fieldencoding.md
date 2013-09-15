This fork adds experimental support for a new feature: "translated field names". 

Field names can take up a significant amount of disk space (as well as bandwidth) in mongodb. 
This fork to tries to remedy this by mapping all field names to short ones which are used in
the collection. The mappings are then stored in a separate collection, only read when the driver first accesses the 
database. 

The code is very simple: A hasmap is used for the mapping, and each time a new field name is used it gets a numeric alias (e.g. "1","2", "3" etc)

Right now there is no support for field name mappings of map/reduce code, but that might come in later. 


Example usage:

 Mongo m=new MongoClient("localhost");
 DB db = m.getDB("db1");
 DBCollection collection = db.getCollection("coll",true);//this is the magic, the 2nd argument, "true", means "translate field names".
 collection.insert(BasicDBObjectBuilder.start().add("name", "elliot").add("address", "east street 2").get());
	

Let's look at what's in the database:
> db.coll.find()
{ "_id" : ObjectId("5188a669c2c49de09d241a43"), "1" : "elliot", "2" : "east street 2" }

OK, "name" is translated to "1" and "address" to "2". Those are nice, short, fieldnames. It's all persisted to "coll.keymap":

> db.coll.keymap.find()
{ "_id" : "name", "shortkey" : "1" }
{ "_id" : "address", "shortkey" : "2" }


Let's read the data back:

DBCursor find = collection.find();
  	while (find.hasNext()){
			DBObject next = find.next();
			System.out.println(next);
		}

And the output:
{ "name" : "elliot" , "address" : "east street 2"}

All looking good, right?

Concurrent access should be handled gracefully (not tested) by a unique index on the coll.keymap "shortkey" field.
