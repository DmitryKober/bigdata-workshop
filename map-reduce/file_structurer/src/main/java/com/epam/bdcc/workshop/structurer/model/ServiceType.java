package com.epam.bdcc.workshop.structurer.model;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
public enum ServiceType {

    DATABASE     ("database"),
    GENERATOR    ("generator"),
    VERIFICATION ("verification"),
    UNKNOWN      ("unknown");

    private String value;

    private ServiceType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static ServiceType fromValue(String value) {
        for (ServiceType serviceType : ServiceType.values()) {
            if (serviceType.value().equals(value)) {
                return serviceType;
            }
        }
        return UNKNOWN;
    }
}
