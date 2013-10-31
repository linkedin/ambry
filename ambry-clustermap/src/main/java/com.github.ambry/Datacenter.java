package com.github.ambry;

// TODO: Datacenter vs DataCenter for name?
/**
 * Datacenter represents a datacenter in Ambry. Each Datacenter is uniquely identifiable by its name.
 */
public class Datacenter {
    private String name; // E.g., "ELA4"

    // "simple" constructor needed for JSON SerDe.
    private Datacenter() {
        this.name = null;
    }

    public Datacenter(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
