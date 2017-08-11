package ru.highloadcup.domain;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class Visit implements WithId {
    public int id;
    public int location;
    public int user;
    public long visited_at;
    public int mark;

    @Override
    public int id() {
        return id;
    }
}
