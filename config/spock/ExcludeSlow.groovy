package spock

runner {
    println "Excluding Slow Spock tests"
    exclude util.spock.annotations.Slow
}
