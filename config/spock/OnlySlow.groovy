package spock

runner {
    println "Only including Slow Spock tests"
    include com.mongodb.spock.Slow
}
