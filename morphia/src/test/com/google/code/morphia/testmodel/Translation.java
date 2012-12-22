package com.google.code.morphia.testmodel;

import com.google.code.morphia.annotations.Embedded;
import java.io.Serializable;

/**
 *
 * @author Olafur Gauti Gudmundsson
 */
@Embedded
public class Translation implements Serializable {
	private static final long serialVersionUID = 1L;

    private String title;
    private String body;

    public Translation() {
    }

    public Translation( String title, String body ) {
        this.title = title;
        this.body = body;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }


}
