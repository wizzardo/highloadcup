package ru.highloadcup;

import com.wizzardo.http.RestHandler;
import com.wizzardo.http.framework.WebApplication;
import com.wizzardo.http.request.Header;
import com.wizzardo.http.response.Status;
import com.wizzardo.tools.io.FileTools;
import com.wizzardo.tools.io.ZipTools;
import com.wizzardo.tools.json.JsonTools;
import com.wizzardo.tools.misc.Stopwatch;
import ru.highloadcup.domain.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class App extends WebApplication {
    Map<Integer, User> users;
    Map<Integer, Location> locations;
    Map<Integer, Visit> visits;

    Map<Integer, List<Visit>> visitsByUser;

    public App(String[] args) {
        super(args);

        onSetup(a -> {
            a.getUrlMapping()
//                    .append("/$entity/$id", new RestHandler().get((request, response) -> response))
                    .append("/users/$id/visits", new RestHandler().get((request, response) -> {
                        int id = request.params().getInt("id", -1);
                        long fromDate = request.params().getLong("fromDate", -1);
                        long toDate = request.params().getLong("toDate", -1);
                        long toDistance = request.params().getInt("toDistance", -1);
                        List<Visit> visitList = visitsByUser.get(id);
                        if (visitList == null)
                            return response.status(Status._404)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);

                        Stream<Visit> stream = visitList.stream();
                        if (fromDate != -1)
                            stream = stream.filter(visit -> visit.visited_at > fromDate); //todo put into tree-map
                        if (toDate != -1)
                            stream = stream.filter(visit -> visit.visited_at < toDate); //todo put into tree-map
                        if (toDistance != -1)
                            stream = stream.filter(visit -> locations.get(visit.location).distance < toDistance); //todo cache

                        List<VisitView> results = stream.sorted(Comparator.comparingLong(o -> o.visited_at))
                                .map(visit -> new VisitView(visit.mark, visit.visited_at, locations.get(visit.location).place))
                                .collect(Collectors.toList());

                        return response
                                .setBody(JsonTools.serialize(results))
                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                .appendHeader(Header.KV_CONNECTION_CLOSE);
                    }))
                    .append("/locations/$id/avg", new RestHandler().get((request, response) -> response.setBody("not impelemnted yet")))
//                    .append("/$entity/$id", new RestHandler().post((request, response) -> response))
//                    .append("/$entity/new", new RestHandler().post((request, response) -> response))
                    .append("/users/$id", new RestHandler()
                            .get((request, response) -> {
                                int id = request.params().getInt("id", -1);
                                User user = users.get(id);
                                if (user == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                else
                                    return response
                                            .setBody(JsonTools.serialize(user))
                                            .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                            })
                            .post((request, response) -> {
                                int id = request.params().getInt("id", -1);
                                if (id == -1)
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                User user = users.get(id);
                                if (user == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                try {
                                    User update = JsonTools.parse(request.getBody().bytes(), User.class);
                                    if (update.email != null)
                                        user.email = update.email;
                                    if (update.first_name != null)
                                        user.first_name = update.first_name;
                                    if (update.last_name != null)
                                        user.last_name = update.last_name;
                                    if (update.gender != null)
                                        user.gender = update.gender;
                                    if (update.birth_date != 0)
                                        user.birth_date = update.birth_date;
                                    return response
                                            .setBody("{}")
                                            .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }
                            })
                    )
                    .append("/locations/$id", new RestHandler()
                            .get((request, response) -> {
                                int id = request.params().getInt("id", -1);
                                Location location = locations.get(id);
                                if (location == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                else
                                    return response
                                            .setBody(JsonTools.serialize(location))
                                            .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                            })
                            .post((request, response) -> {
                                int id = request.params().getInt("id", -1);
                                if (id == -1)
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                Location location = locations.get(id);
                                if (location == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                try {
                                    Location update = JsonTools.parse(request.getBody().bytes(), Location.class);
                                    if (update.place != null)
                                        location.place = update.place;
                                    if (update.country != null)
                                        location.country = update.country;
                                    if (update.city != null)
                                        location.city = update.city;
                                    if (update.distance != 0)
                                        location.distance = update.distance;
                                    return response
                                            .setBody("{}")
                                            .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }
                            })
                    )
                    .append("/visits/$id", new RestHandler()
                            .get((request, response) -> {
                                int id = request.params().getInt("id", -1);
                                Visit visit = visits.get(id);
                                if (visit == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                else
                                    return response
                                            .setBody(JsonTools.serialize(visit))
                                            .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                            })
                            .post((request, response) -> {
                                int id = request.params().getInt("id", -1);
                                if (id == -1)
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                Visit visit = visits.get(id);
                                if (visit == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                try {
                                    Visit update = JsonTools.parse(request.getBody().bytes(), Visit.class);
                                    if (update.location != 0)
                                        visit.location = update.location;
                                    if (update.user != 0)
                                        visit.user = update.user;
                                    if (update.mark != 0)
                                        visit.mark = update.mark;
                                    if (update.visited_at != 0)
                                        visit.visited_at = update.visited_at;
                                    return response
                                            .setBody("{}")
                                            .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }
                            })
                    )
                    .append("/users/new", new RestHandler().post((request, response) -> {
                        try {
                            User update = JsonTools.parse(request.getBody().bytes(), User.class);
                            User user = users.get(update.id);
                            if (user != null)
                                return response.status(Status._404)
                                        .appendHeader(Header.KV_CONNECTION_CLOSE);

                            users.put(update.id, update); //todo update other collections
                            return response
                                    .setBody("{}")
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        } catch (Exception e) {
                            return response.status(Status._400)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        }
                    }))
                    .append("/locations/new", new RestHandler().post((request, response) -> {
                        try {
                            Location update = JsonTools.parse(request.getBody().bytes(), Location.class);
                            Location location = locations.get(update.id);
                            if (location != null)
                                return response.status(Status._404)
                                        .appendHeader(Header.KV_CONNECTION_CLOSE);

                            locations.put(update.id, update); //todo update other collections
                            return response
                                    .setBody("{}")
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        } catch (Exception e) {
                            return response.status(Status._400)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        }
                    }))
                    .append("/visits/new", new RestHandler().post((request, response) -> {
                        try {
                            Visit update = JsonTools.parse(request.getBody().bytes(), Visit.class);
                            Visit visit = visits.get(update.id);
                            if (visit != null)
                                return response.status(Status._404)
                                        .appendHeader(Header.KV_CONNECTION_CLOSE);

                            visits.put(update.id, update); //todo update other collections
                            return response
                                    .setBody("{}")
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        } catch (Exception e) {
                            return response.status(Status._400)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        }
                    }))
            ;

            Stopwatch stopwatch = new Stopwatch("initialized in");
            initData();
            System.out.println(stopwatch);
            System.out.println("users: " + users.size());
            System.out.println("locations: " + locations.size());
            System.out.println("visits: " + visits.size());
            System.out.println("visits by user: " + visitsByUser.size());
        });
    }

    public static void main(String[] args) {
        App app = new App(args);
        app.start();
    }

    private void initData() {
        File dest = new File("/tmp/data_unzipped");
        ZipTools.unzip(new File("/tmp/data/data.zip"), dest);
        Locations locations = new Locations();
        Users users = new Users();
        Visits visits = new Visits();
        for (File file : dest.listFiles()) {
            System.out.println("reading data from " + file);
            if (file.getName().startsWith("locations"))
                locations.locations.addAll(JsonTools.parse(FileTools.bytes(file), Locations.class).locations);
            else if (file.getName().startsWith("users"))
                users.users.addAll(JsonTools.parse(FileTools.bytes(file), Users.class).users);
            else if (file.getName().startsWith("visits"))
                visits.visits.addAll(JsonTools.parse(FileTools.bytes(file), Visits.class).visits);
            else
                throw new IllegalArgumentException("Unknown file: " + file);
        }

        this.users = initMap(users.users);
        this.locations = initMap(locations.locations);
        this.visits = initMap(visits.visits);

        this.visitsByUser = new HashMap<>(1024, 1f);
        for (Visit visit : visits.visits) {
            List<Visit> visitList = visitsByUser.computeIfAbsent(visit.user, integer -> new ArrayList<>(16));
            visitList.add(visit);
        }
    }

    protected <T extends WithId> Map<Integer, T> initMap(List<T> data) {
        Map<Integer, T> map = new ConcurrentHashMap<>(data.size() + 1, 1f);
        for (T t : data) {
            map.put(t.id(), t);
        }
        return map;
    }
}
