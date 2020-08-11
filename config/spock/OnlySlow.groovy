package spock

runner {
    println "Only including Slow Spock tests"
    include util.spock.annotations.Slow
}
