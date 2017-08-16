package ru.highloadcup.domain;

import com.wizzardo.epoll.readable.ReadableByteBuffer;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class Visit implements WithId {
    public int id;
    public int location;
    public int user;
    public long visited_at;
    public int mark;

    public transient ReadableByteBuffer staticResponse;

    @Override
    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "Visit{" +
                "id=" + id +
                ", location=" + location +
                ", user=" + user +
                ", visited_at=" + visited_at +
                ", mark=" + mark +
                '}';
    }
}
