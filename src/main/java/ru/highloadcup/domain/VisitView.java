package ru.highloadcup.domain;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class VisitView {
    public int mark;
    public long visited_at;
    public String place;

    public VisitView(int mark, long visited_at, String place) {
        this.mark = mark;
        this.visited_at = visited_at;
        this.place = place;
    }
}
