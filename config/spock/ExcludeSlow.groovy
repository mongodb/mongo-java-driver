package spock

runner {
    println "Excluding Slow Spock tests"
    exclude com.mongodb.spock.Slow
}
