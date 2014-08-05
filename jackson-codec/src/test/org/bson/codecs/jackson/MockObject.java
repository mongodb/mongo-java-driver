package org.bson.codecs.jackson;/*
 * Copyright 2011 VZ Netzwerke Ltd
 * Copyright 2014 devbliss GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.bson.BsonJavaScript;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Test object with one of each field
 */
public class MockObject {

    public BsonJavaScript js;
    public BsonSymbol symbol;
    public BsonTimestamp ts;
    public Pattern regex;
    public Date date;
    public ObjectId oid;
    public String _id;
    public String string;
    public Object nulls;
    public Integer integer;
    public Long longs;
//    public String utf8String;
//    public BigInteger bigInteger;
//    public Float floats;
//    public BigDecimal bigDecimal;
    public Double doubles;
    public Boolean booleans;
    public ArrayList<Object> arrays;
    public MockObject obj;


    public MockObject() {
        doubles = -4.0;
    }

    public MockObject(boolean mockFields) {
        if (mockFields) {
            js = new BsonJavaScript("var a = 1;");
            symbol = new BsonSymbol("someSymbol");
            ts = new BsonTimestamp(10,20);
            regex = Pattern.compile("pattern");
            _id = "this is an unique ID";
            obj = new MockObject();
            string = "this is a string";
            integer = -1;
            longs = -2L;
            doubles = -3.0;
            booleans = true;
            nulls = null;
            arrays = new ArrayList<Object>();
            arrays.add(10);
            arrays.add(null);
            arrays.add(new ArrayList<Integer>());
            arrays.add("this is a string in an array");

//            bigInteger = BigInteger.TEN;
//            bigDecimal = BigDecimal.TEN;
//            utf8String = "this is (not) a utf8 string (yet)";
        }
    }

    @Override
    public String toString() {
        return "org.bson.codecs.jackson.MockObject{" +
                "js='" + js + "'" +
                ", symbol='" + symbol + "'" +
                ", ts='" + ts + "'" +
                ", regex='" + regex.pattern() + "'" +
                ", date=" + date + "'" +
                ", oid='" + oid + "'" +
                ", _id='" + _id + "'" +
                ", string='" + string + "'" +
                ", integer=" + integer +
                ", longs=" + longs +
//                ", bigInteger=" + bigInteger +
//                ", floats=" + floats +
                ", doubles=" + doubles +
//                ", bigDecimal=" + bigDecimal +
                ", booleans=" + booleans +
                ", arrays=" +  arrays +
                ", obj=" + obj +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MockObject that = (MockObject) o;

        if (_id != null ? !_id.equals(that._id) : that._id != null) {
            return false;
        }
        if (oid != null ? !oid.equals(that.oid) : that.oid != null) {
            return false;
        }
        if (regex.pattern() != null ? !regex.pattern().equals(that.regex.pattern()) : that.regex.pattern() != null) {
            return false;
        }
//        if (bigDecimal != null ? !bigDecimal.equals(that.bigDecimal) : that.bigDecimal != null) {
//            return false;
//        }
//        if (bigInteger != null ? !bigInteger.equals(that.bigInteger) : that.bigInteger != null) {
//            return false;
//        }
        if (booleans != null ? !booleans.equals(that.booleans) : that.booleans != null) {
            return false;
        }
        if (doubles != null ? !doubles.equals(that.doubles) : that.doubles != null) {
            return false;
        }
//        if (floats != null ? !floats.equals(that.floats) : that.floats != null) {
//            return false;
//        }
        if (integer != null ? !integer.equals(that.integer) : that.integer != null) {
            return false;
        }
        if (longs != null ? !longs.equals(that.longs) : that.longs != null) {
            return false;
        }
        if (date != null ? !date.equals(that.date) : that.date != null) {
            return false;
        }
        if (arrays != null ? !arrays.equals(that.arrays) : that.arrays != null) {
            return false;
        }
        if (string != null ? !string.equals(that.string) : that.string != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = _id != null ? _id.hashCode() : 0;
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + (oid != null ? oid.hashCode() : 0);
        result = 31 * result + (integer != null ? integer.hashCode() : 0);
        result = 31 * result + (longs != null ? longs.hashCode() : 0);
//        result = 31 * result + (bigInteger != null ? bigInteger.hashCode() : 0);
//        result = 31 * result + (floats != null ? floats.hashCode() : 0);
        result = 31 * result + (doubles != null ? doubles.hashCode() : 0);
//        result = 31 * result + (bigDecimal != null ? bigDecimal.hashCode() : 0);
        result = 31 * result + (booleans != null ? booleans.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (arrays != null ? arrays.hashCode() : 0);
        result = 31 * result + (regex != null ? regex.hashCode() : 0);
        return result;
    }
}
