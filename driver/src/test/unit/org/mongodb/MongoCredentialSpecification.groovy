package org.mongodb

import spock.lang.Specification

import static org.mongodb.AuthenticationMechanism.MONGODB_CR;
import static org.mongodb.AuthenticationMechanism.PLAIN;

class MongoCredentialSpecification extends Specification {
    def 'creating a challenge-response credential should populate correct fields'() {
        given:
        AuthenticationMechanism mechanism = MONGODB_CR;
        String userName = 'user';
        String database = 'test';
        char[] password = 'pwd'.toCharArray();

        when:
        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);

        then:
        mechanism == credential.getMechanism()
        userName == credential.getUserName()
        database == credential.getSource()
        password == credential.getPassword()
        MONGODB_CR == credential.getMechanism()
    }

    def 'should throw IllegalArgumentException when required parameter is not supplied for challenge-response'() {
        when:
        MongoCredential.createMongoCRCredential(null, 'test', 'pwd'.toCharArray());
        then:
        thrown(IllegalArgumentException)

        when:
        MongoCredential.createMongoCRCredential('user', null, 'pwd'.toCharArray());
        then:
        thrown(IllegalArgumentException)

        when:
        MongoCredential.createMongoCRCredential('user', 'test', null);
        then:
        thrown(IllegalArgumentException)
    }

    def 'creating a Plain credential should populate all required fields'() {
        given:
        AuthenticationMechanism mechanism = PLAIN;
        String userName = 'user';
        char[] password = 'pwd'.toCharArray();

        when:
        MongoCredential credential = MongoCredential.createPlainCredential(userName, password);

        then:
        mechanism == credential.getMechanism()
        userName == credential.getUserName()
        '$external' == credential.getSource()
        password == credential.getPassword()
        mechanism == credential.getMechanism()
    }

    def 'should throw IllegalArgumentException when a required field is not passed in'() {
        when:
        MongoCredential.createPlainCredential(null, 'pwd'.toCharArray());
        then:
        thrown(IllegalArgumentException)

        when:
        MongoCredential.createPlainCredential('user', null);
        then:
        thrown(IllegalArgumentException)
    }

    def 'creating a GSSAPI Credential should populate the correct fields'() {
        given:
        AuthenticationMechanism mechanism = AuthenticationMechanism.GSSAPI;
        String userName = 'user';

        when:
        MongoCredential credential = MongoCredential.createGSSAPICredential(userName);

        then:
        mechanism == credential.getMechanism()
        userName == credential.getUserName()
        '$external' == credential.getSource()
        null == credential.getPassword()
    }

    def 'should throw IllegalArgumentException if username is not provided to a GSSAPI credential'() {
        when:
        MongoCredential.createGSSAPICredential(null);

        then:
        thrown(IllegalArgumentException)
    }

    def 'testObjectOverrides'() {
        String userName = 'user';
        String database = 'test';
        char[] password = 'pwd'.toCharArray();

        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);
        MongoCredential.createMongoCRCredential(userName, database, password) == credential
        MongoCredential.createMongoCRCredential(userName, database, password).hashCode() == credential.hashCode()
        "MongoCredential{mechanism='MONGODB-CR', userName='user', source='test', password=<hidden>}" == credential.toString()
    }

}
