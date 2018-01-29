/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities;

public final class PrimitivesModel {

    private boolean myBoolean;
    private byte myByte;
    private char myCharacter;
    private double myDouble;
    private float myFloat;
    private int myInteger;
    private long myLong;
    private short myShort;

    public PrimitivesModel() {
    }

    public PrimitivesModel(final boolean myBoolean, final byte myByte, final char myCharacter, final double myDouble,
                           final float myFloat, final int myInteger, final long myLong, final short myShort) {
        this.myBoolean = myBoolean;
        this.myByte = myByte;
        this.myCharacter = myCharacter;
        this.myDouble = myDouble;
        this.myFloat = myFloat;
        this.myInteger = myInteger;
        this.myLong = myLong;
        this.myShort = myShort;
    }

    public boolean isMyBoolean() {
        return myBoolean;
    }

    public void setMyBoolean(final boolean myBoolean) {
        this.myBoolean = myBoolean;
    }

    public byte getMyByte() {
        return myByte;
    }

    public void setMyByte(final byte myByte) {
        this.myByte = myByte;
    }

    public char getMyCharacter() {
        return myCharacter;
    }

    public void setMyCharacter(final char myCharacter) {
        this.myCharacter = myCharacter;
    }

    public double getMyDouble() {
        return myDouble;
    }

    public void setMyDouble(final double myDouble) {
        this.myDouble = myDouble;
    }

    public float getMyFloat() {
        return myFloat;
    }

    public void setMyFloat(final float myFloat) {
        this.myFloat = myFloat;
    }

    public int getMyInteger() {
        return myInteger;
    }

    public void setMyInteger(final int myInteger) {
        this.myInteger = myInteger;
    }

    public long getMyLong() {
        return myLong;
    }

    public void setMyLong(final long myLong) {
        this.myLong = myLong;
    }

    public short getMyShort() {
        return myShort;
    }

    public void setMyShort(final short myShort) {
        this.myShort = myShort;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PrimitivesModel that = (PrimitivesModel) o;

        if (isMyBoolean() != that.isMyBoolean()) {
            return false;
        }
        if (getMyByte() != that.getMyByte()) {
            return false;
        }
        if (getMyCharacter() != that.getMyCharacter()) {
            return false;
        }
        if (Double.compare(that.getMyDouble(), getMyDouble()) != 0) {
            return false;
        }
        if (Float.compare(that.getMyFloat(), getMyFloat()) != 0) {
            return false;
        }
        if (getMyInteger() != that.getMyInteger()) {
            return false;
        }
        if (getMyLong() != that.getMyLong()) {
            return false;
        }
        if (getMyShort() != that.getMyShort()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (isMyBoolean() ? 1 : 0);
        result = 31 * result + (int) getMyByte();
        result = 31 * result + (int) getMyCharacter();
        temp = Double.doubleToLongBits(getMyDouble());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (getMyFloat() != +0.0f ? Float.floatToIntBits(getMyFloat()) : 0);
        result = 31 * result + getMyInteger();
        result = 31 * result + (int) (getMyLong() ^ (getMyLong() >>> 32));
        result = 31 * result + (int) getMyShort();
        return result;
    }
}
