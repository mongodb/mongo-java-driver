h2. Change Log h2.

2009-02-14 : add DBCollection.insert(DBObject[]) and DBCollection.insert(List<DBObject>) methods - doing a bulk insert
can really boost performance.

2009-02-13 : Make it clear that the license is Apache License 2.0

2009-02-12 : remove "seen" to avoid memory overhead on running over large collections.  Downside is that this exposes
a small bug in MongoDB if you happen to modify a document in a large collection while iterating, but that will soon
be fixed.



