
2.7.0-rc0 / 2011-10-24 
=======================

  * JAVA-434: replaced isEmpty() with 1.5 compatible alternative
  * JAVA-363: NPE in GridFS.validate
  * JAVA-356: make JSONParseException public
  * JAVA-413: intern dbref.ns
  * JAVA-444: query before insert on index creation.
  * JAVA-404: slaveOk support for inline mapreduce (routes to secondaries); changed CommandResult to include serverUsed, made readPref-Secondary set slaveOk query flag, toString/toJSON on Node/ReplicaSetStatus.
  * Import javax.net in an OSGi environment.
  * JAVA-428 - Support new ReadPreference semantics, deprecate SlaveOK 
  * Added a skip method to ensure continueOnInsertError test is only run on Server versions 2.0+
  * JAVA-448 - Tailable Cursor behavior while not in AWAIT mode 
    + Fixed non-await empty tailable cursor behavior to be more consistently inline with other drivers & user expectations. Instead of forcing a sleep of 500 milliseconds on "no data", we instead when tailable catch an empty cursor and return null instead.  This should be more safely non blocking for users who need to roll their own event driven code, for which the sleep breaks logic.
  * JAVA-439: Check keys to disallow dot in key names for embedded maps
  * added getObjectId method.
  * add partial query option flag
  * JAVA-425 - Support MongoDB Server 2.0 getLastError Changes (j, w=string/number) 
  * JAVA-427 - Support ContinueOnError Flag for bulk insert 
  * JAVA-422 - Memoize field locations (and other calculated data) for LazyBSONObject
  * JAVA-421 - LazyBSONObject exhibits certain behavioral breakages
  * JAVA-420: LazyBSONObject does not handle DBRef
  * Fix & cleanup of  Javadoc comments (kevinsawicki)
  * JAVA-365: Poor concurrency handling in DBApiLayer.doGetCollection
  * JAVA-364: MapReduceOutput sometimes returns empty results in a replica set when SLAVE_OK=true
  * JAVA-333: Allow bson decoder per operation (find/cursor)
  * JAVA-323: When executing a command, only toplevel object returned should be a CommandResult (not sub-objects)

