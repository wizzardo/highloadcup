package ru.highloadcup.domain;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class Location implements WithId {

    public int id;
    public String place;
    public String country;
    public String city;
    public int distance;

    @Override
    public int id() {
        return id;
    }
}
