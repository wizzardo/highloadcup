package ru.highloadcup.domain;

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

    public enum Gender {
        m, f;
    }

    @Override
    public int id() {
        return id;
    }
}
