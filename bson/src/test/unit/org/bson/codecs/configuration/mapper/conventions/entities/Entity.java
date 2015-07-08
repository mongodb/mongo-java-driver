/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson.codecs.configuration.mapper.conventions.entities;

public class Entity {
    private String name;
    private String fullName;
    private Integer faves;
    private Long age;
    private Boolean debug = false;

    public Entity() {
    }

    public Entity(final Long age, final int faves, final String name, final String fullName) {
        this.age = age;
        this.faves = faves;
        this.name = name;
        this.fullName = fullName;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(final Long age) {
        this.age = age;
    }

    public Boolean getDebug() {
        return debug;
    }

    public Integer getFaves() {
        return faves;
    }

    public void setFaves(final Integer faves) {
        this.faves = faves;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(final String fullName) {
        this.fullName = fullName;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + faves;
        result = 31 * result + (age != null ? age.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Entity entity = (Entity) o;

        if (faves != entity.faves) {
            return false;
        }
        if (name != null ? !name.equals(entity.name) : entity.name != null) {
            return false;
        }
        if (age != null ? !age.equals(entity.age) : entity.age != null) {
            return false;
        }

        return true;
    }
}
