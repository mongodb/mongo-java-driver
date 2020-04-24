## Contributing to the MongoDB Java Driver

Thank you for your interest in contributing to the MongoDB Java driver.

We are building this software together and strongly encourage contributions from the community that are within the guidelines set forth 
below. 

Bug Fixes and New Features
--------------------------

Before starting to write code, look for existing [tickets](https://jira.mongodb.org/browse/JAVA) or 
[create one](https://jira.mongodb.org/secure/CreateIssue!default.jspa) for your bug, issue, or feature request. This helps the community 
avoid working on something that might not be of interest or which has already been addressed.

Pull Requests
-------------

Pull requests should generally be made against the master (default) branch and include relevant tests, if applicable. 

Code should compile with the Java 9 compiler and tests should pass under all Java versions which the driver currently
supports. Currently the Java driver supports a minimum version of Java 8.  Please run './gradlew test' to confirm.   By default, running the
tests requires that you start a mongod server on localhost, listening on the default port and configured to run with
[`enableTestCommands`](http://docs.mongodb.org/manual/reference/parameters/#param.enableTestCommands), which may be set with the 
`--setParameter enableTestCommands=1` command-line parameter.   At minimum, please test against the latest release version of the MongoDB 
server.

The results of pull request testing will be appended to the request. If any tests do not pass, or relevant tests are not included, the 
pull request will not be considered. 

Talk To Us
----------

If you have questions about using the driver, please reach out on the 
[MongoDB Community Forums](https://developer.mongodb.com/community/forums/tags/c/drivers-odms-connectors/7/java-driver).
