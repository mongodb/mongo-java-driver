
2.7.3 / 2012-01-30 
==================

  * synchronized access to encoder/decoder
  * JAVA-505: Made encoder creation work just like decoder creation
    + using cached DBDecoder in most cases to avoid excessive memory allocation caused by too many instances of DefaultDBDecoder being created
  * JAVA-505 / JAVA-421 - Regression in performance of Java Driver should be rolled back (GC Related)

2.7.3RC1 / 2012-01-17 
==================

  * Remove defunct BSONInputTest
  * JAVA-505 / JAVA-421 - Regression in performance of Java Driver should be rolled back (GC Related)

2.7.2 / 2011-11-10 
==================

  * JAVA-469: java.net.NetworkInterface.getNetworkInterfaces may fail with IBM JVM, which prevents from using driver
  * deprecated replica pair constructors. 
    - updated javadocs and removed replica pair javadocs in class doc.
  * JAVA-428: Fixed an issue where read preferences not set on cursor

2.7.1 / 2011-11-08 
==================

  * JAVA-467 - added _check call to getServerAddress if _it is null 
  * JAVA-467 - Moved variable calls to method to fix read preference hierarchy 
  * simplified getServerAddress method.

2.7.0 / 2011-11-04 
===================

 * Released Java Driver 2.7.0
 * See change notes from Release Candidate series
 * Please report any bugs or issues to https://jira.mongodb.org/browse/JAVA


2.7.0-rc4 / 2011-11-03 
=======================

  * New Secondary Selection tests for Replica Sets introduced
  * To correct a regression, make WriteConcern immutable (Otherwise you can mutate static presets like SAFE at runtime) * Reintroduced constructors which accept continueOnErrorForInsert args * To enable you to set continueOnErrorForInsert with the presets like SAFE, immutable "new WriteConcern like this with COEI changed" method added.

2.7.0-rc3 / 2011-10-31 
=======================

  * changed if statement to improve readability.
  * JAVA-462: GridFSInputFile does not close input streams when constructed by the driver (greenlaw110)
    - Add closeStreamOnPersist option when creating GridFSInputFile
  * JAVA-425 fixes
    - attempt to clean up and standardize writeConcern
    - throw exception if w is wrong type
    - fix cast exception in case W is a String
  * Documented continue on error better.
  * Close inputstream of GridFSInputFile once chunk saved
  * JAVA-461: the logic to spread requests around slaves may select a slave over latency limit
  * Reset buffer when the object is checked out and before adding back.
  * added MongoOptions test
  * use the socket factory from the Mongo for ReplicaSetStatus connections
  * added MongoOptions.copy

2.7.0-rc2 / 2011-10-26 
========================

  * JAVA-459: smooth the latency measurements (for secondary/slave server selection)
  * JAVA-428: Fixed edge cases where slaveOK / ReadPreference.SECONDARY wasn't working
  * JAVA-444: make ensureIndex first do a read on index collection to see if index exists * If an embedded field was referenced in an index a hard failure   occurred,  due to triggering of the 'no dots in key names' logic   via the insert.  Moved code to use the lower level API method which   permits disabling of key checks.
  * Introduced "bamboo-test" task which does NOT halt on failure to allow bamboo tests to complete and report properly (and various fixes)
  * added unit test for x.y ensureIndex.

2.7.0-rc1 / 2011-10-24 
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

