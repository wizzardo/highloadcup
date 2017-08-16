package ru.highloadcup;

import ru.highloadcup.domain.Location;
import ru.highloadcup.domain.User;
import ru.highloadcup.domain.Visit;

import java.nio.charset.StandardCharsets;

/**
 * Created by Mikhail Bobrutskov on 12.08.17.
 */
public class VisitInfo {
    Visit visit;
    Location location;
    User user;

    protected int mark;
    protected long visitedAt;
    protected String place;
    protected byte[] json;

    public VisitInfo(Visit visit, Location location, User user) {
        this.visit = visit;
        this.location = location;
        this.user = user;
        json = prepareJson();
    }

    private byte[] prepareJson() {
        return ("{\"mark\":" + (mark = visit.mark) + ",\"visited_at\":" + (visitedAt = visit.visited_at) + ",\"place\":\"" + (place = location.place) + "\"}")
                .getBytes(StandardCharsets.UTF_8);
    }

    public byte[] getJson() {
        if (mark == visit.mark && visitedAt == visit.visited_at && place == location.place)
            return json;
        else
            return json = prepareJson();
    }

    @Override
    public String toString() {
        return "VisitInfo{" +
                "visit=" + visit +
                ", location=" + location +
                ", user=" + user +
                '}';
    }
}
