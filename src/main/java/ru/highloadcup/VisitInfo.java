package ru.highloadcup;

import ru.highloadcup.domain.Location;
import ru.highloadcup.domain.User;
import ru.highloadcup.domain.Visit;

/**
 * Created by Mikhail Bobrutskov on 12.08.17.
 */
public class VisitInfo {
    Visit visit;
    Location location;
    User user;

    public VisitInfo(Visit visit, Location location, User user) {
        this.visit = visit;
        this.location = location;
        this.user = user;
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
