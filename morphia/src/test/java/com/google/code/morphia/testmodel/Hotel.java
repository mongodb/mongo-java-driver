/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia.testmodel;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.testutil.TestEntity;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

/**
 *
 * @author Olafur Gauti Gudmundsson
 */
@Entity("hotels")
public class Hotel extends TestEntity {
	private static final long serialVersionUID = 1L;

	public static Hotel create() {
		return new Hotel();
	}
	
    public enum Type { BUSINESS, LEISURE }

    private String name;
    private Date startDate;
    private int stars;
    private boolean takesCreditCards;
    private Type type;
    private Set<String> tags;

    @Transient
    private String temp;

    @Embedded
    private Address address;

    @Embedded(concreteClass = Vector.class)
    private List<PhoneNumber> phoneNumbers;

    private Hotel() {
        super();
        tags = new HashSet<String>();
        phoneNumbers = new Vector<PhoneNumber>();
    }

    
    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStars() {
        return stars;
    }

    public void setStars(int stars) {
        this.stars = stars;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public boolean isTakesCreditCards() {
        return takesCreditCards;
    }

    public void setTakesCreditCards(boolean takesCreditCards) {
        this.takesCreditCards = takesCreditCards;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    public List<PhoneNumber> getPhoneNumbers() {
        return phoneNumbers;
    }

    public void setPhoneNumbers(List<PhoneNumber> phoneNumbers) {
        this.phoneNumbers = phoneNumbers;
    }

    public String getTemp() {
        return temp;
    }

    public void setTemp(String temp) {
        this.temp = temp;
    }
}
