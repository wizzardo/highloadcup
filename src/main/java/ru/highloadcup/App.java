package ru.highloadcup;

import com.wizzardo.epoll.readable.ReadableByteBuffer;
import com.wizzardo.http.FileTreeHandler;
import com.wizzardo.http.RestHandler;
import com.wizzardo.http.framework.WebApplication;
import com.wizzardo.http.request.Header;
import com.wizzardo.http.response.Response;
import com.wizzardo.http.response.Status;
import com.wizzardo.tools.http.Request;
import com.wizzardo.tools.interfaces.Mapper;
import com.wizzardo.tools.io.IOTools;
import com.wizzardo.tools.json.JsonItem;
import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;
import com.wizzardo.tools.misc.Stopwatch;
import com.wizzardo.tools.misc.TextTools;
import com.wizzardo.tools.misc.Unchecked;
import ru.highloadcup.domain.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
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
    long[] years;

    AtomicBoolean posts = new AtomicBoolean(false);

    public App(String[] args) {
        super(args);

        Stopwatch stopwatch = new Stopwatch("initialized in");
        initData();
        System.out.println(stopwatch);
        System.out.println("users: " + users.size());
        System.out.println("locations: " + locations.size());
        System.out.println("visits: " + visits.size());
        System.out.println("visits by user: " + visitsByUser.size());
        System.out.println("visits by location: " + visitsByLocation.size());

        List<VisitInfo> maxVisitsByUser = getMaxVisistsByUser();
        System.out.println("max visits by user." + maxVisitsByUser.get(0).user.id + ": " + maxVisitsByUser.size());
        List<VisitInfo> maxVisitsByLocation = getMaxVisitsByLocation();
        System.out.println("max visits by location." + maxVisitsByLocation.get(0).location.id + ": " + maxVisitsByLocation.size());

        onSetup(a -> {
//            ReadableByteBuffer static_400 = new Response().status(Status._400).buildStaticResponse();
            Mapper<Response, Response> response_400 = (response) -> response.status(Status._400);

            a.getUrlMapping()
                    .append("/users/$id/visits", new RestHandler().get((request, response) -> {
                        unmarkPosts();
                        addCloseIfNotKeepAlive(request, response);

                        int id = request.params().getInt("id", -1);
                        List<VisitInfo> visitList = visitsByUser.get(id);
                        if (visitList == null)
                            return response.status(Status._404)
                                    ;

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
                            return response_400.map(response);
                        }

                        List<VisitView> results = stream.sorted(Comparator.comparingLong(o -> o.visit.visited_at))
                                .map(visitInfo -> new VisitView(visitInfo.visit.mark, visitInfo.visit.visited_at, visitInfo.location.place))
                                .collect(Collectors.toList());

//                        if (results.isEmpty())
//                            return response.status(Status._404)
//                                    ;

                        return response
                                .setBody(serializeJson(new VisitViews(results)))
                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
//
                                ;
                    }))
                    .append("/locations/$id/avg", new RestHandler().get((request, response) -> {
                        unmarkPosts();
                        addCloseIfNotKeepAlive(request, response);
                        int id = request.params().getInt("id", -1);
                        List<VisitInfo> visitList = visitsByLocation.get(id);
                        if (visitList == null)
                            return response.status(Status._404)
                                    ;

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
                                long ageValue = getSecondsFromAge(fromAge);
//                                long ageValue = System.currentTimeMillis() - fromAge * 1000l * 60 * 60 * 24 * 365;
                                stream = stream.filter(visit -> visit.user.birth_date < ageValue);
                            }
                            s = request.param("toAge");
                            if (s != null) {
                                int toAge = Integer.parseInt(s);
                                long ageValue = getSecondsFromAge(toAge);
//                                long ageValue = System.currentTimeMillis() - toAge * 1000l * 60 * 60 * 24 * 365;
                                stream = stream.filter(visit -> visit.user.birth_date > ageValue);
                            }
                            s = request.param("gender");
                            if (s != null) {
                                User.Gender gender = User.Gender.valueOf(s);
                                stream = stream.filter(visit -> visit.user.gender == gender);
                            }

                        } catch (Exception e) {
                            return response_400.map(response);
                        }

                        OptionalDouble average = stream
                                .mapToDouble(visitInfo -> visitInfo.visit.mark)
                                .average();

                        return response
                                .setBody(serializeJson(new Average(Math.round(average.orElse(0) * 100000) / 100000d)))
                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                ;
                    }))
                    .append("/users/$id", new RestHandler()
                            .get((request, response) -> {
                                unmarkPosts();
                                addCloseIfNotKeepAlive(request, response);
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._404);
                                }

                                User user = users.get(id);
                                if (user == null)
                                    return response.status(Status._404);

                                return response.setStaticResponse(user.staticResponse.copy());
                            })
                            .post((request, response) -> {
                                markPosts();
                                addCloseIfNotKeepAlive(request, response);

                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._404);
                                }

                                User user = users.get(id);
                                if (user == null)
                                    return response.status(Status._404);

                                try {
                                    JsonObject update = JsonTools.parse(request.getBody().bytes()).asJsonObject();
                                    for (JsonItem item : update.values()) {
                                        if (item.isNull())
                                            return response_400.map(response);
                                    }

                                    JsonItem item;
                                    if ((item = update.get("email")) != null) {
                                        user.email = item.asString();
                                    }
                                    if ((item = update.get("first_name")) != null) {
                                        user.first_name = item.asString();
                                    }
                                    if ((item = update.get("last_name")) != null) {
                                        user.last_name = item.asString();
                                    }
                                    if ((item = update.get("gender")) != null) {
                                        user.gender = User.Gender.valueOf(item.asString());
                                    }
                                    if ((item = update.get("birth_date")) != null) {
                                        user.birth_date = item.asLong();
                                    }
                                } catch (Exception e) {
                                    return response_400.map(response);
                                }

                                user.staticResponse = prepareStaticJsonResponse(user);
                                return response
                                        .setBody("{}")
                                        .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
                            })
                    )
                    .append("/locations/$id", new RestHandler()
                            .get((request, response) -> {
                                unmarkPosts();
                                addCloseIfNotKeepAlive(request, response);
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._404)
                                            ;
                                }

                                Location location = locations.get(id);
                                if (location == null)
                                    return response.status(Status._404);

                                return response.setStaticResponse(location.staticResponse.copy());
                            })
                            .post((request, response) -> {
                                markPosts();
                                addCloseIfNotKeepAlive(request, response);
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response.status(Status._404)
                                            ;
                                }

                                Location location = locations.get(id);
                                if (location == null)
                                    return response.status(Status._404)
                                            ;

                                try {
                                    JsonObject update = JsonTools.parse(request.getBody().bytes()).asJsonObject();
                                    for (JsonItem item : update.values()) {
                                        if (item.isNull())
                                            return response_400.map(response);
                                    }

                                    JsonItem item;
                                    if ((item = update.get("country")) != null) {
                                        location.country = item.asString();
                                    }
                                    if ((item = update.get("city")) != null) {
                                        location.city = item.asString();
                                    }
                                    if ((item = update.get("place")) != null) {
                                        location.place = item.asString();
                                    }
                                    if ((item = update.get("distance")) != null) {
                                        location.distance = item.asInteger();
                                    }
                                } catch (Exception e) {
                                    return response_400.map(response);
                                }

                                location.staticResponse = prepareStaticJsonResponse(location);
                                return response
                                        .setBody("{}")
                                        .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                        ;
                            })
                    )
                    .append("/visits/$id", new RestHandler()
                            .get((request, response) -> {
                                unmarkPosts();
                                addCloseIfNotKeepAlive(request, response);
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response_400.map(response);
                                }
                                Visit visit = visits.get(id);
                                if (visit == null)
                                    return response.status(Status._404);

                                return response.setStaticResponse(visit.staticResponse.copy());
                            })
                            .post((request, response) -> {
                                markPosts();
                                addCloseIfNotKeepAlive(request, response);
                                int id;
                                try {
                                    id = Integer.parseInt(request.param("id"));
                                } catch (Exception e) {
                                    return response_400.map(response);
                                }

                                Visit visit = visits.get(id);
                                if (visit == null)
                                    return response.status(Status._404)
                                            ;

                                try {
                                    JsonObject update = JsonTools.parse(request.getBody().bytes()).asJsonObject();
                                    for (JsonItem item : update.values()) {
                                        if (item.isNull())
                                            return response_400.map(response);
                                    }

                                    JsonItem item;
                                    if ((item = update.get("user")) != null) {
                                        removeFromVisitMaps(visit);
                                        visit.user = item.asInteger();
                                        addToVisitMaps(visit);
                                    }
                                    if ((item = update.get("location")) != null) {
                                        removeFromVisitMaps(visit);
                                        visit.location = item.asInteger();
                                        addToVisitMaps(visit);
                                    }
                                    if ((item = update.get("mark")) != null) {
                                        visit.mark = item.asInteger();
                                    }
                                    if ((item = update.get("visited_at")) != null) {
                                        visit.visited_at = item.asLong();
                                    }
                                } catch (Exception e) {
                                    return response_400.map(response);
                                }

                                visit.staticResponse = prepareStaticJsonResponse(visit);
                                return response
                                        .setBody("{}")
                                        .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                        ;
                            })
                    )
                    .append("/users/new", new RestHandler().post((request, response) -> {
                        markPosts();
                        addCloseIfNotKeepAlive(request, response);
                        try {
                            UserUpdate update = JsonTools.parse(request.getBody().bytes(), UserUpdate.class);
                            if (update.id == null || update.birth_date == null || update.first_name == null
                                    || update.last_name == null || update.email == null || update.gender == null)
                                return response_400.map(response);

                            User user = users.get(update.id);
                            if (user != null)
                                return response_400.map(response);

                            user = new User();
                            user.birth_date = update.birth_date;
                            user.first_name = update.first_name;
                            user.last_name = update.last_name;
                            user.id = update.id;
                            user.gender = update.gender;
                            user.email = update.email;

                            users.put(update.id, user);
                            visitsByUser.computeIfAbsent(update.id, integer -> new ArrayList<>(16));
                            user.staticResponse = prepareStaticJsonResponse(user);
                            return response
                                    .setBody("{}")
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                    ;
                        } catch (Exception e) {
                            return response_400.map(response);
                        }
                    }))
                    .append("/locations/new", new RestHandler().post((request, response) -> {
                        markPosts();
                        addCloseIfNotKeepAlive(request, response);
                        try {
                            LocationUpdate update = JsonTools.parse(request.getBody().bytes(), LocationUpdate.class);
                            if (update.id == null || update.city == null || update.country == null
                                    || update.place == null || update.distance == null)
                                return response_400.map(response);

                            Location location = locations.get(update.id);
                            if (location != null)
                                return response_400.map(response);

                            location = new Location();
                            location.id = update.id;
                            location.distance = update.distance;
                            location.city = update.city;
                            location.country = update.country;
                            location.place = update.place;
                            locations.put(update.id, location);
                            visitsByLocation.computeIfAbsent(update.id, integer -> new ArrayList<>(16));
                            location.staticResponse = prepareStaticJsonResponse(location);
                            return response
                                    .setBody("{}")
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                    ;
                        } catch (Exception e) {
                            return response_400.map(response);
                        }
                    }))
                    .append("/visits/new", new RestHandler().post((request, response) -> {
                        markPosts();
                        addCloseIfNotKeepAlive(request, response);
                        try {
                            Visit update = JsonTools.parse(request.getBody().bytes(), Visit.class);
                            Visit visit = visits.get(update.id);
                            if (visit != null)
                                return response_400.map(response);

                            visits.put(update.id, update);
                            addToVisitMaps(update);
                            update.staticResponse = prepareStaticJsonResponse(update);
                            return response
                                    .setBody("{}")
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                                    ;
                        } catch (Exception e) {
                            return response_400.map(response);
                        }
                    }))
            ;
        });
    }

    public List<VisitInfo> getMaxVisitsByLocation() {
        return visitsByLocation.values().stream().max(Comparator.comparingInt(List::size)).get();
    }

    public List<VisitInfo> getMaxVisistsByUser() {
        return visitsByUser.values().stream().max(Comparator.comparingInt(List::size)).get();
    }

    public byte[] serializeJson(Object data) {
        return JsonTools.serializeToBytes(data);
    }

    public void addCloseIfNotKeepAlive(com.wizzardo.http.request.Request request, Response response) {
        if (!request.connection().isKeepAlive())
            response.appendHeader(Header.KV_CONNECTION_CLOSE);
    }

    public void removeFromVisitMaps(Visit visit) {
        visitsByUser.get(visit.user).removeIf(visitInfo -> visitInfo.visit.id == visit.id);
        visitsByLocation.get(visit.location).removeIf(visitInfo -> visitInfo.visit.id == visit.id);
    }

    public void markPosts() {
        if (posts.compareAndSet(false, true)) {
            System.out.println("============== POSTS ==============");
        }
    }

    public void unmarkPosts() {
        if (posts.compareAndSet(true, false)) {
            System.out.println("============== GETS 2 ==============");
        }
    }

    public void addToVisitMaps(Visit visit) {
        addToVisitsByUser(visit);
        addToVisitsByLocation(visit);
    }

    public void addToVisitsByLocation(Visit visit) {
        visitsByLocation.computeIfAbsent(visit.location, integer -> new CopyOnWriteArrayList<>())
                .add(createVisitInfo(visit));
    }

    public VisitInfo createVisitInfo(Visit visit) {
        return new VisitInfo(visit, locations.get(visit.location), users.get(visit.user));
    }

    public void addToVisitsByUser(Visit visit) {
        visitsByUser.computeIfAbsent(visit.user, integer -> new CopyOnWriteArrayList<>())
                .add(createVisitInfo(visit));
    }

    public long getSecondsFromAge(int age) {
        if (age < years.length)
            return years[age];
        else
            return getSecondsOfAge(age);
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        App app = new App(args);
        app.start();
        float startupTime = (System.currentTimeMillis() - time) / 1000f;
        System.out.println("App started in " + startupTime + " seconds");

        time = System.currentTimeMillis();
        warmUp(app, 20 - startupTime);
        System.out.println("warmUp finished in " + (System.currentTimeMillis() - time) / 1000f + " seconds");
    }

    public static String formatBytes(long bytes) {
        return bytes / 1024 / 1024 + " mb";
    }

    private static void warmUp(App app, float seconds) {
        int user = app.getMaxVisistsByUser().get(0).user.id;
        long time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) / 1000 < seconds) {
            String exec = Unchecked.ignore(() -> exec("ab -c 2 -k -n 10000 http://127.0.0.1:" + app.getPort() + "/users/" + user + "/visits"), "");
            System.out.println(TextTools.find(exec, Pattern.compile("Requests per second:\\s+\\d+.\\d+")));
//            System.out.println(exec);
            Unchecked.ignore(() -> Thread.sleep(2000));
        }
    }

    private void initData() {
//        File dest = new File("/tmp/data_unzipped");
        File zipFile = new File("/tmp/data/data.zip");
//        ZipTools.unzip(zipFile, dest);
        Locations locations = new Locations();
        Users users = new Users();
        Visits visits = new Visits();
        AtomicLong sizeCounter = new AtomicLong();
        Unchecked.run(() -> {
            ZipInputStream zip = new ZipInputStream(new FileInputStream(zipFile));
            ZipEntry nextEntry;
            byte[] buffer = new byte[65536];
            ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024);
            while ((nextEntry = zip.getNextEntry()) != null) {
                String name = nextEntry.getName();
//                Stopwatch stopwatch = new Stopwatch("reading data from " + name, true);
                sizeCounter.addAndGet(nextEntry.getSize());
                IOTools.copy(zip, out, buffer);
                String json = out.toString();
                out.reset();
//                System.out.println(stopwatch);

//                stopwatch = new Stopwatch("parsing data from " + name, true);
                if (name.startsWith("locations"))
                    locations.locations.addAll(JsonTools.parse(json, Locations.class).locations);
                else if (name.startsWith("users"))
                    users.users.addAll(JsonTools.parse(json, Users.class).users);
                else if (name.startsWith("visits"))
                    visits.visits.addAll(JsonTools.parse(json, Visits.class).visits);
                else
                    throw new IllegalArgumentException("Unknown file: " + name);
//                System.out.println(stopwatch);
            }
            IOTools.close(zip);
        });
        System.out.println("total jsons size: " + sizeCounter.get() / 1024 / 1024 + " MB");

        this.users = initMap(users.users);
        this.locations = initMap(locations.locations);
        this.visits = initMap(visits.visits);

//        Stopwatch stopwatch = new Stopwatch("init maps of visits by location and user", true);
        this.visitsByUser = new ConcurrentHashMap<>(this.users.size() + 1, 1f);
        this.visitsByLocation = new ConcurrentHashMap<>(this.locations.size() + 1, 1f);
        for (Visit visit : visits.visits) {
            addToVisitMaps(visit);
        }
        for (User user : users.users) {
            visitsByUser.computeIfAbsent(user.id, integer -> new ArrayList<>(16));
        }
        for (Location location : locations.locations) {
            visitsByLocation.computeIfAbsent(location.id, integer -> new ArrayList<>(16));
        }
//        System.out.println(stopwatch);

        for (User user : users.users) {
            user.staticResponse = prepareStaticJsonResponse(user);
        }
        for (Location location : locations.locations) {
            location.staticResponse = prepareStaticJsonResponse(location);
        }

        for (Visit visit : visits.visits) {
            visit.staticResponse = prepareStaticJsonResponse(visit);
        }

        long[] years = new long[256];
        for (int i = 0; i < years.length; i++) {
            years[i] = getSecondsOfAge(i);
//            System.out.println("age " + i + ": " + years[i]);
        }
        this.years = years;
    }

    private ReadableByteBuffer prepareStaticJsonResponse(Object ob) {
        return new Response()
                .setBody(serializeJson(ob))
//                .appendHeader("Server: wizzardo-http/0.1\r\n".getBytes())
                .appendHeader(Header.KV_CONNECTION_CLOSE)
//                    .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                .buildStaticResponse();
    }

    private long getSecondsOfAge(int i) {
        return LocalDateTime.now().minus(i, ChronoUnit.YEARS).toEpochSecond(ZoneOffset.UTC); // todo reset current time
    }

    protected <T extends WithId> Map<Integer, T> initMap(List<T> data) {
//        Stopwatch stopwatch = new Stopwatch("init map of " + data.get(0).getClass(), true);
        Map<Integer, T> map = new ConcurrentHashMap<>(data.size() + 1, 1f);
        for (T t : data) {
            map.put(t.id(), t);
        }
//        System.out.println(stopwatch);
        return map;
    }

    public static String exec(String cmd) {
        try {
            Process process;
            process = Runtime.getRuntime().exec(cmd);
            int result = process.waitFor();
//            if (result != 0)
//                throw new IllegalArgumentException("Process ended with code: " + result);

            byte[] bytes = new byte[1024];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = process.getInputStream();
            InputStream err = process.getErrorStream();
            int read;
            while ((read = in.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
            in.close();
            out.write("\n".getBytes());
            while ((read = err.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
            err.close();
            out.close();
            return new String(out.toByteArray(), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
