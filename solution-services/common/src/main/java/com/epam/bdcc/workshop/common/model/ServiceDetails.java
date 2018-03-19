package com.epam.bdcc.workshop.common.model;

import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * Created by Dmitrii_Kober on 3/14/2018.
 */
@JsonRootName("serviceDetails")
public class ServiceDetails {

    private String description;

    public ServiceDetails(String description) {
        this.description = description;
    }

    public ServiceDetails() {
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
