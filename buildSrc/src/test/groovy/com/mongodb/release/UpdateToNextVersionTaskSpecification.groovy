package com.mongodb.release

import spock.lang.Specification

class UpdateToNextVersionTaskSpecification extends Specification {
    def 'should parse a string to correctly increment the version number'() {
        when:
        def newVersion = UpdateToNextVersionTask.incrementToNextVersion('0.109')
        
        then:
        newVersion == '0.110-SNAPSHOT'
    }

    def 'should parse a string to correctly increment the version number over a boundary'() {
        when:
        def newVersion = UpdateToNextVersionTask.incrementToNextVersion('0.110')
        
        then:
        newVersion == '0.111-SNAPSHOT'
    }

}
