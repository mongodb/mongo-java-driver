group = "org.mongodb"
description = "This Bill of Materials POM simplifies dependency management when referencing multiple" +
        " MongoDB Java Driver artifacts in projects using Gradle or Maven."

dependencies {
    constraints {
        api(project(":mongodb-crypt"))
        api(project(":driver-core"))
        api(project(":bson"))
        api(project(":bson-record-codec"))

        api(project(":driver-sync"))
        api(project(":driver-reactive-streams"))

        api(project(":bson-kotlin"))
        api(project(":bson-kotlinx"))
        api(project(":driver-kotlin-coroutine"))
        api(project(":driver-kotlin-sync"))

        api(project(":bson-scala"))
        api(project(":driver-scala"))

    }
}