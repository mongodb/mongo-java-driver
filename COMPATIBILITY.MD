### Incompatibilities with MongoDB 4.2

MongoDB 4.2 introduced the "NonResumableChangeStreamError" error label, which is not recognized
by the 4.0-era Java driver.  This may cause the 4.0-era driver to resume a change stream in cases
where the server has indicated that it should not. This may cause iteration of a change stream to 
loop infinitely instead of reporting an error to the application
                                              
### Incompatibilities with MongoDB 4.4

MongoDB 4.4 introduced the "ResumableChangeStreamError" error label, which is not recognized
by the 4.0-era Java driver. This may cause the 4.0-era driver to not resume a change stream in cases
where the server has indicated that it should.  

MongoDB 4.4 removed support for mapReduce statistics.  The 4.0-era driver assumes statistics are always
included in the reply, so any use of mapReduce with the 4.0-era driver will fail.  MongoDB 4.4 also 
removed support for the default values of sharded and nonAtomic fields for mapReduce to a collection.
The 4.0-era driver always includes the default values of these fields (even if unset by the application), 
so this will cause any mapReduce with $out to fail as well.

