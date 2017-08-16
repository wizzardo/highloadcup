package ru.highloadcup.domain;

import com.wizzardo.epoll.readable.ReadableByteBuffer;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class User implements WithId {
    public int id;
    public String email;
    public String first_name;
    public String last_name;
    public long birth_date;
    public Gender gender;

    public transient ReadableByteBuffer staticResponse;

    public enum Gender {
        m, f;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", email='" + email + '\'' +
                ", first_name='" + first_name + '\'' +
                ", last_name='" + last_name + '\'' +
                ", birth_date=" + birth_date +
                ", gender=" + gender +
                '}';
    }
}
