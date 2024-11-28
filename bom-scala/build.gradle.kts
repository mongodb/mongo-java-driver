group = "org.mongodb"
base.archivesName.set("mongodb-driver-bom")
description = "This Bill of Materials POM simplifies dependency management when referencing multiple" +
        " MongoDB Scala Driver artifacts in projects using Gradle or Maven."

dependencies {
    constraints {
        api(project(":mongodb-crypt"))
        api(project(":driver-core"))
        api(project(":driver-sync"))
        api(project(":driver-reactive-streams"))
        api(project(":bson"))
        api(project(":bson-record-codec"))

        api(project(":driver-reactive-streams"))
        api(project(":bson"))
        api(project(":bson-scala"))
        api(project(":driver-scala"))
    }
}