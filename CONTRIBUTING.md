## Contributing to the MongoDB Java Driver

Thank you for your interest in contributing to the MongoDB Java driver.

We are building this software together and strongly encourage contributions
from the community that are within the guidelines set forth below.

Bug Fixes and New Features
--------------------------

Before starting to write code, look for existing [tickets]
(https://jira.mongodb.org/browse/JAVA) or [create one]
(https://jira.mongodb.org/secure/CreateIssue!default.jspa) 
for your bug, issue, or feature request. This helps the community
avoid working on something that might not be of interest or which
has already been addressed.

Pull Requests
-------------

Pull requests should be made against the master (development)
branch and include relevant tests, if applicable. The driver follows
the Git-Flow branching model where the traditional master branch is
known as release and the master (default) branch is considered under
development.

Code should compile and tests should pass under all Java versions 
which the driver currently supports.  Currently the Java driver supports
a minimum version of Java 5.  Please run 'ant test' to confirm.  If your
tests modify code related to replica sets, please ensure that you run the
tests with a replica set where the primary is on port 27017.

The results of pull request testing will be appended to the request.
If any tests do not pass, or relevant tests are not included the pull
request will not be considered.

Talk To Us
----------

If you want to work on something or have questions / complaints please reach
out to us by creating a Question issue at (https://jira.mongodb.org/secure/CreateIssue!default.jspa).
