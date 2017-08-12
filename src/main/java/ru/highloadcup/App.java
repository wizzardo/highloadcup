package ru.highloadcup;

import com.wizzardo.http.RestHandler;
import com.wizzardo.http.framework.WebApplication;
import com.wizzardo.http.request.Header;
import com.wizzardo.http.response.Status;
import com.wizzardo.tools.http.Request;
import com.wizzardo.tools.io.IOTools;
import com.wizzardo.tools.json.JsonTools;
import com.wizzardo.tools.misc.Stopwatch;
import com.wizzardo.tools.misc.Unchecked;
import ru.highloadcup.domain.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class App extends WebApplication {
    Map<Integer, User> users;
    Map<Integer, Location> locations;
    Map<Integer, Visit> visits;

    Map<Integer, List<VisitInfo>> visitsByUser;
    Map<Integer, List<VisitInfo>> visitsByLocation;

    public App(String[] args) {
        super(args);

        Stopwatch stopwatch = new Stopwatch("initialized in");
        initData();
        System.out.println(stopwatch);
        System.out.println("users: " + users.size());
        System.out.println("locations: " + locations.size());
        System.out.println("visits: " + visits.size());
        System.out.println("visits by user: " + visitsByUser.size());

//        System.out.println();
//        System.out.println("user.1031: " + users.get(1031));
//        System.out.println("visits by user.1031: " + visitsByUser.get(1031));
//        System.out.println();
//        System.out.println("location.132: " + locations.get(132));
//        System.out.println("visits by location.132: " + visitsByLocation.get(132));
//        System.out.println();
//        System.out.println("location.218: " + locations.get(218));
//        System.out.println("visits by location.218: " + visitsByLocation.get(218));
//        System.out.println();

        onSetup(a -> {
            a.getUrlMapping()
                    .append("/users/$id/visits", new RestHandler().get((request, response) -> {
                        int id = request.params().getInt("id", -1);
                        List<VisitInfo> visitList = visitsByUser.get(id);
                        if (visitList == null)
                            return response.status(Status._404)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);

                        Stream<VisitInfo> stream = visitList.stream();
                        try {
                            String s;
                            s = request.param("fromDate");
                            if (s != null) {
                                long fromDate = Long.parseLong(s);
                                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at > fromDate); //todo put into tree-map
                            }
                            s = request.param("toDate");
                            if (s != null) {
                                long toDate = Long.parseLong(s);
                                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at < toDate); //todo put into tree-map
                            }
                            s = request.param("toDistance");
                            if (s != null) {
                                long toDistance = Integer.parseInt(s);
                                stream = stream.filter(visitInfo -> visitInfo.location.distance < toDistance);
                            }
                            s = request.param("country");
                            if (s != null) {
                                String country = s;
                                stream = stream.filter(visitInfo -> country.equals(visitInfo.location.country));
                            }
                        } catch (Exception e) {
                            return response.status(Status._400)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        }

                        List<VisitView> results = stream.sorted(Comparator.comparingLong(o -> o.visit.visited_at))
                                .map(visitInfo -> new VisitView(visitInfo.visit.mark, visitInfo.visit.visited_at, visitInfo.location.place))
                                .collect(Collectors.toList());

//                        if (results.isEmpty())
//                            return response.status(Status._404)
//                                    .appendHeader(Header.KV_CONNECTION_CLOSE);

                        return response
                                .setBody(JsonTools.serialize(new VisitViews(results)))
                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                .appendHeader(Header.KV_CONNECTION_CLOSE);
                    }))
                    .append("/locations/$id/avg", new RestHandler().get((request, response) -> {
                        int id = request.params().getInt("id", -1);
                        List<VisitInfo> visitList = visitsByLocation.get(id);
                        if (visitList == null)
                            return response.status(Status._404)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);

                        Stream<VisitInfo> stream = visitList.stream();
                        try {
                            String s;
                            s = request.param("fromDate");
                            if (s != null) {
                                long fromDate = Long.parseLong(s);
                                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at > fromDate); //todo put into tree-map
                            }
                            s = request.param("toDate");
                            if (s != null) {
                                long toDate = Long.parseLong(s);
                                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at < toDate); //todo put into tree-map
                            }
                            s = request.param("fromAge");
                            if (s != null) {
                                int fromAge = Integer.parseInt(s);
                                long ageValue = LocalDateTime.now().minus(fromAge, ChronoUnit.YEARS).toEpochSecond(ZoneOffset.UTC);
//                                long ageValue = System.currentTimeMillis() - fromAge * 1000l * 60 * 60 * 24 * 365;
                                stream = stream.filter(visit -> visit.user.birth_date < ageValue);
                            }
                            s = request.param("toAge");
                            if (s != null) {
                                int toAge = Integer.parseInt(s);
                                long ageValue = LocalDateTime.now().minus(toAge, ChronoUnit.YEARS).toEpochSecond(ZoneOffset.UTC);
//                                long ageValue = System.currentTimeMillis() - toAge * 1000l * 60 * 60 * 24 * 365;
                                stream = stream.filter(visit -> visit.user.birth_date > ageValue);
                            }
                            s = request.param("gender");
                            if (s != null) {
                                User.Gender gender = User.Gender.valueOf(s);
                                stream = stream.filter(visit -> visit.user.gender == gender);
                            }

                        } catch (Exception e) {
                            return response.status(Status._400)
                                    .appendHeader(Header.KV_CONNECTION_CLOSE);
                        }

                        OptionalDouble average = stream
                                .mapToDouble(visitInfo -> visitInfo.visit.mark)
                                .average();

//                        if (!average.isPresent())
//                            return response.status(Status._404)
//                                    .appendHeader(Header.KV_CONNECTION_CLOSE);

                        return response
                                .setBody(JsonTools.serialize(new Average(Math.round(average.orElse(0) * 100000) / 100000d)))
                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                .appendHeader(Header.KV_CONNECTION_CLOSE);
                    }))
                    .append("/users/$id", new RestHandler()
                            .get((request, response) -> {
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }

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
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }

                                User user = users.get(id);
                                if (user == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                User update;
                                try {
                                    update = JsonTools.parse(request.getBody().bytes(), User.class);
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }

                                System.out.println("update user, data: " + new String(request.getBody().bytes()));

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

                                request.connection().setCloseOnFinishWriting(true);
                                return response
                                        .setBody("{}")
                                        .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                        .appendHeader(Header.KV_CONNECTION_CLOSE);
                            })
                    )
                    .append("/locations/$id", new RestHandler()
                            .get((request, response) -> {
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }

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
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }

                                Location location = locations.get(id);
                                if (location == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                Location update;
                                try {
                                    update = JsonTools.parse(request.getBody().bytes(), Location.class);
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }

                                if (update.place != null)
                                    location.place = update.place;
                                if (update.country != null)
                                    location.country = update.country;
                                if (update.city != null)
                                    location.city = update.city;
                                if (update.distance != 0)
                                    location.distance = update.distance;

                                request.connection().setCloseOnFinishWriting(true);
                                return response
                                        .setBody("{}")
                                        .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                        .appendHeader(Header.KV_CONNECTION_CLOSE);
                            })
                    )
                    .append("/visits/$id", new RestHandler()
                            .get((request, response) -> {
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }
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
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }

                                Visit visit = visits.get(id);
                                if (visit == null)
                                    return response.status(Status._404)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);

                                Visit update;
                                try {
                                    update = JsonTools.parse(request.getBody().bytes(), Visit.class);
                                } catch (Exception e) {
                                    return response.status(Status._400)
                                            .appendHeader(Header.KV_CONNECTION_CLOSE);
                                }
                                if (update.location != 0)
                                    visit.location = update.location;
                                if (update.user != 0)
                                    visit.user = update.user;
                                if (update.mark != 0)
                                    visit.mark = update.mark;
                                if (update.visited_at != 0)
                                    visit.visited_at = update.visited_at;

                                request.connection().setCloseOnFinishWriting(true);
                                return response
                                        .setBody("{}")
                                        .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                        .appendHeader(Header.KV_CONNECTION_CLOSE);
                            })
                    )
                    .append("/users/new", new RestHandler().post((request, response) -> {
                        try {
                            User update = JsonTools.parse(request.getBody().bytes(), User.class);
                            System.out.println("new user: " + update);
                            User user = users.get(update.id);
                            if (user != null)
                                return response.status(Status._404)
                                        .appendHeader(Header.KV_CONNECTION_CLOSE);

                            users.put(update.id, update); //todo update other collections
                            request.connection().setCloseOnFinishWriting(true);
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
                            request.connection().setCloseOnFinishWriting(true);
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
                            request.connection().setCloseOnFinishWriting(true);
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
        });
    }

    public static void main(String[] args) {
        Stopwatch stopwatch = new Stopwatch("App started in", true);
        App app = new App(args);
        app.start();
        System.out.println(stopwatch);
        warmUp(app);
    }

    private static void warmUp(App app) {
        long time = System.nanoTime();
        int counter = 0;
        for (Map.Entry<Integer, User> entry : app.users.entrySet()) {
            counter++;
            Unchecked.ignore(() -> new Request("http://localhost:8080/users/" + entry.getValue().id).get());
        }
        for (Map.Entry<Integer, Location> entry : app.locations.entrySet()) {
            counter++;
            Unchecked.ignore(() -> new Request("http://localhost:8080/locations/" + entry.getValue().id).get());
        }
        for (Map.Entry<Integer, Visit> entry : app.visits.entrySet()) {
            counter++;
            Unchecked.ignore(() -> new Request("http://localhost:8080/visits/" + entry.getValue().id).get());
        }
        time = System.nanoTime() - time;
        System.out.println("warmUp finished in " + (time / 1000 / 1000 / 1000f) + " s");
        System.out.println("average response time is " + (time / counter) / 1000f + " us");
    }

    private void initData() {
//        File dest = new File("/tmp/data_unzipped");
        File zipFile = new File("/tmp/data/data.zip");
//        ZipTools.unzip(zipFile, dest);
        Locations locations = new Locations();
        Users users = new Users();
        Visits visits = new Visits();
        Unchecked.run(() -> {
            ZipInputStream zip = new ZipInputStream(new FileInputStream(zipFile));
            ZipEntry nextEntry;
            byte[] buffer = new byte[65536];
            ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024);
            while ((nextEntry = zip.getNextEntry()) != null) {
                String name = nextEntry.getName();
                Stopwatch stopwatch = new Stopwatch("reading data from " + name, true);
                IOTools.copy(zip, out, buffer);
                String json = out.toString();
                out.reset();
                System.out.println(stopwatch);

                stopwatch = new Stopwatch("parsing data from " + name, true);
                if (name.startsWith("locations"))
                    locations.locations.addAll(JsonTools.parse(json, Locations.class).locations);
                else if (name.startsWith("users"))
                    users.users.addAll(JsonTools.parse(json, Users.class).users);
                else if (name.startsWith("visits"))
                    visits.visits.addAll(JsonTools.parse(json, Visits.class).visits);
                else
                    throw new IllegalArgumentException("Unknown file: " + name);
                System.out.println(stopwatch);
            }
            IOTools.close(zip);
        });

        this.users = initMap(users.users);
        this.locations = initMap(locations.locations);
        this.visits = initMap(visits.visits);

        Stopwatch stopwatch = new Stopwatch("init maps of visits by location and user", true);
        this.visitsByUser = new HashMap<>(this.users.size() + 1, 1f);
        this.visitsByLocation = new HashMap<>(this.locations.size() + 1, 1f);
        for (Visit visit : visits.visits) {
            visitsByUser.computeIfAbsent(visit.user, integer -> new ArrayList<>(16))
                    .add(new VisitInfo(visit, this.locations.get(visit.location), this.users.get(visit.user)));
            visitsByLocation.computeIfAbsent(visit.location, integer -> new ArrayList<>(16))
                    .add(new VisitInfo(visit, this.locations.get(visit.location), this.users.get(visit.user)));
        }
        for (User user : users.users) {
            visitsByUser.computeIfAbsent(user.id, integer -> new ArrayList<>(16));
        }
        for (Location location : locations.locations) {
            visitsByLocation.computeIfAbsent(location.id, integer -> new ArrayList<>(16));
        }
        System.out.println(stopwatch);
    }

    protected <T extends WithId> Map<Integer, T> initMap(List<T> data) {
        Stopwatch stopwatch = new Stopwatch("init map of " + data.get(0).getClass(), true);
        Map<Integer, T> map = new ConcurrentHashMap<>(data.size() + 1, 1f);
        for (T t : data) {
            map.put(t.id(), t);
        }
        System.out.println(stopwatch);
        return map;
    }
}
