package ru.highloadcup.domain;

import com.wizzardo.epoll.readable.ReadableByteBuffer;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class Location implements WithId {

    public int id;
    public String place;
    public String country;
    public String city;
    public int distance;

    public transient ReadableByteBuffer staticResponse;

    @Override
    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "Location{" +
                "id=" + id +
                ", place='" + place + '\'' +
                ", country='" + country + '\'' +
                ", city='" + city + '\'' +
                ", distance=" + distance +
                '}';
    }
}
