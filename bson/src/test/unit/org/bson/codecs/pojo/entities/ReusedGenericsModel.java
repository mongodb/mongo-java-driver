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

public final class ReusedGenericsModel<A, B, C> {

    private A field1;
    private B field2;
    private C field3;
    private Integer field4;
    private C field5;
    private B field6;
    private A field7;
    private String field8;

    public ReusedGenericsModel() {
    }

    public ReusedGenericsModel(final A field1, final B field2, final C field3, final Integer field4, final C field5, final B field6,
                               final A field7, final String field8) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
        this.field5 = field5;
        this.field6 = field6;
        this.field7 = field7;
        this.field8 = field8;
    }

    /**
     * Returns the field1
     *
     * @return the field1
     */
    public A getField1() {
        return field1;
    }

    public void setField1(final A field1) {
        this.field1 = field1;
    }

    /**
     * Returns the field2
     *
     * @return the field2
     */
    public B getField2() {
        return field2;
    }

    public void setField2(final B field2) {
        this.field2 = field2;
    }

    /**
     * Returns the field3
     *
     * @return the field3
     */
    public C getField3() {
        return field3;
    }

    public void setField3(final C field3) {
        this.field3 = field3;
    }

    /**
     * Returns the field4
     *
     * @return the field4
     */
    public Integer getField4() {
        return field4;
    }

    public void setField4(final Integer field4) {
        this.field4 = field4;
    }

    /**
     * Returns the field5
     *
     * @return the field5
     */
    public C getField5() {
        return field5;
    }

    public void setField5(final C field5) {
        this.field5 = field5;
    }

    /**
     * Returns the field6
     *
     * @return the field6
     */
    public B getField6() {
        return field6;
    }

    public void setField6(final B field6) {
        this.field6 = field6;
    }

    /**
     * Returns the field7
     *
     * @return the field7
     */
    public A getField7() {
        return field7;
    }

    public void setField7(final A field7) {
        this.field7 = field7;
    }

    /**
     * Returns the field8
     *
     * @return the field8
     */
    public String getField8() {
        return field8;
    }

    public void setField8(final String field8) {
        this.field8 = field8;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReusedGenericsModel)) {
            return false;
        }

        ReusedGenericsModel<?, ?, ?> that = (ReusedGenericsModel<?, ?, ?>) o;

        if (getField1() != null ? !getField1().equals(that.getField1()) : that.getField1() != null) {
            return false;
        }
        if (getField2() != null ? !getField2().equals(that.getField2()) : that.getField2() != null) {
            return false;
        }
        if (getField3() != null ? !getField3().equals(that.getField3()) : that.getField3() != null) {
            return false;
        }
        if (getField4() != null ? !getField4().equals(that.getField4()) : that.getField4() != null) {
            return false;
        }
        if (getField5() != null ? !getField5().equals(that.getField5()) : that.getField5() != null) {
            return false;
        }
        if (getField6() != null ? !getField6().equals(that.getField6()) : that.getField6() != null) {
            return false;
        }
        if (getField7() != null ? !getField7().equals(that.getField7()) : that.getField7() != null) {
            return false;
        }
        if (getField8() != null ? !getField8().equals(that.getField8()) : that.getField8() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getField1() != null ? getField1().hashCode() : 0;
        result = 31 * result + (getField2() != null ? getField2().hashCode() : 0);
        result = 31 * result + (getField3() != null ? getField3().hashCode() : 0);
        result = 31 * result + (getField4() != null ? getField4().hashCode() : 0);
        result = 31 * result + (getField5() != null ? getField5().hashCode() : 0);
        result = 31 * result + (getField6() != null ? getField6().hashCode() : 0);
        result = 31 * result + (getField7() != null ? getField7().hashCode() : 0);
        result = 31 * result + (getField8() != null ? getField8().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ReusedGenericsModel{"
                + "field1=" + field1
                + ", field2=" + field2
                + ", field3=" + field3
                + ", field4=" + field4
                + ", field5=" + field5
                + ", field6=" + field6
                + ", field7=" + field7
                + ", field8='" + field8 + "'"
                + "}";
    }
}
