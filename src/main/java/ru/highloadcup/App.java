package ru.highloadcup;

import com.wizzardo.epoll.readable.ReadableByteBuffer;
import com.wizzardo.http.HttpConnection;
import com.wizzardo.http.MultiValue;
import com.wizzardo.http.RestHandler;
import com.wizzardo.http.framework.WebApplication;
import com.wizzardo.http.request.Header;
import com.wizzardo.http.request.Request;
import com.wizzardo.http.response.Response;
import com.wizzardo.http.response.Status;
import com.wizzardo.tools.interfaces.Mapper;
import com.wizzardo.tools.io.FileTools;
import com.wizzardo.tools.io.IOTools;
import com.wizzardo.tools.json.JsonItem;
import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;
import com.wizzardo.tools.misc.ExceptionDrivenStringBuilder;
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
import java.util.concurrent.ThreadLocalRandom;
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
    protected static final byte[] HEADER_SERVER_NAME = "Server: wizzardo-http/0.1\r\n".getBytes();
    Map<Integer, User> users;
    Map<Integer, Location> locations;
    Map<Integer, Visit> visits;

    Map<Integer, List<VisitInfo>> visitsByUser;
    Map<Integer, List<VisitInfo>> visitsByLocation;
    long[] years;

    long dataLength;

    @Override
    protected void handle(HttpConnection connection) throws Exception {
        Request request = connection.getRequest();
        Response response = connection.getResponse();

        connection.setKeepAlive(true);

        response.appendHeader(HEADER_SERVER_NAME);
        response.appendHeader(serverDate.getDateAsBytes());
        response.appendHeader(Header.KV_CONNECTION_KEEP_ALIVE);

        handle(request, response);
    }

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

        List<VisitInfo> maxVisitsByUser = getMaxVisitsByUser();
        if (!maxVisitsByUser.isEmpty())
            System.out.println("max visits by user." + maxVisitsByUser.get(0).user.id + ": " + maxVisitsByUser.size());

        List<VisitInfo> maxVisitsByLocation = getMaxVisitsByLocation();
        if (!maxVisitsByLocation.isEmpty())
            System.out.println("max visits by location." + maxVisitsByLocation.get(0).location.id + ": " + maxVisitsByLocation.size());

        onSetup(a -> {
//            ReadableByteBuffer static_400 = new Response().status(Status._400).buildStaticResponse();
            Mapper<Response, Response> response_400 = (response) -> response.status(Status._400);
            //todo make static 400, 404, 200 for POST
            //todo don't read headers on GET -> ignore all headers except Content-Length
            //todo check if i can put data into arrays instead of maps
            //todo GC tweaks? -> pool of arrays for responses
            //todo remove framework filters
            //todo check custom mapping performance

            a.getUrlMapping()
                    .append("/users/$id/visits", new RestHandler().get((request, response) -> {
//                        addCloseIfNotKeepAlive(request, response);

                        int id = request.params().getInt("id", -1);
                        List<VisitInfo> visitList = visitsByUser.get(id);
                        if (visitList == null)
                            return response.status(Status._404);

                        Stream<VisitInfo> stream = visitList.stream();
                        try {
                            stream = prepareUserVisitsStream(request, stream);
                        } catch (Exception e) {
                            return response_400.map(response);
                        }

                        List<VisitInfo> results = stream.sorted(Comparator.comparingLong(o -> o.visit.visited_at)) //todo make collection sorted by default
//                                .map(visitInfo -> new VisitView(visitInfo.visit.mark, visitInfo.visit.visited_at, visitInfo.location.place))
                                .collect(Collectors.toList());

                        byte[] result;
                        if (results.isEmpty()) {
                            result = new byte[]{'{', '"', 'v', 'i', 's', 'i', 't', 's', '"', ':', '[', ']', '}'};
                        } else {
                            int size = 1 + 2 + 2 + 1 + 6; // {"visits":[ + ] - last ',' }
                            for (VisitInfo vi : results) {
                                size += vi.getJson().length + 1;
                            }
                            result = new byte[size];
                            result[0] = '{';
                            result[1] = '"';
                            result[2] = 'v';
                            result[3] = 'i';
                            result[4] = 's';
                            result[5] = 'i';
                            result[6] = 't';
                            result[7] = 's';
                            result[8] = '"';
                            result[9] = ':';
                            result[10] = '[';
                            int offset = 11;
                            for (VisitInfo vi : results) {
                                System.arraycopy(vi.json, 0, result, offset, vi.json.length);
                                offset += vi.json.length; // ignore checks
                                result[offset++] = ',';
                            }
                            result[offset - 1] = ']';
                            result[offset] = '}';
                        }

                        return response
                                .setBody(result)
                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
                    }))
                    .append("/locations/$id/avg", new RestHandler().get((request, response) -> {
//                        addCloseIfNotKeepAlive(request, response);
                        int id = request.params().getInt("id", -1);
                        List<VisitInfo> visitList = visitsByLocation.get(id);
                        if (visitList == null)
                            return response.status(Status._404)
                                    ;

                        Stream<VisitInfo> stream = visitList.stream();
                        try {
                            stream = prepareLocationVisitsStream(request, stream);
                        } catch (Exception e) {
                            return response_400.map(response);
                        }

                        OptionalDouble average = stream
                                .mapToDouble(visitInfo -> visitInfo.visit.mark)
                                .average();

                        double result = Math.round(average.orElse(0) * 100000) / 100000d;

                        String s = new ExceptionDrivenStringBuilder().append("{\"avg\":").append(result).append("}").toString();
                        return response
                                .setBody(s)
                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
                    }))
                    .append("/users/$id", new RestHandler()
                                    .get((request, response) -> {
//                                addCloseIfNotKeepAlive(request, response);
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
//                                addCloseIfNotKeepAlive(request, response);

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
                                            if (!parseUserUpdate(request, user))
                                                return response_400.map(response);
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
//                                addCloseIfNotKeepAlive(request, response);
                                        int id;
                                        try {
                                            id = Integer.parseInt(request.param("id"));
                                        } catch (Exception e) {
                                            return response.status(Status._404);
                                        }

                                        Location location = locations.get(id);
                                        if (location == null)
                                            return response.status(Status._404);

                                        return response.setStaticResponse(location.staticResponse.copy());
                                    })
                                    .post((request, response) -> {
//                                addCloseIfNotKeepAlive(request, response);
                                        int id;
                                        try {
                                            id = Integer.parseInt(request.param("id"));
                                        } catch (Exception e) {
                                            return response.status(Status._404);
                                        }

                                        Location location = locations.get(id);
                                        if (location == null)
                                            return response.status(Status._404);

                                        try {
                                            if (!parseLocationUpdate(request, location))
                                                return response_400.map(response);
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
//                                addCloseIfNotKeepAlive(request, response);
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
//                                addCloseIfNotKeepAlive(request, response);
                                        int id;
                                        try {
                                            id = Integer.parseInt(request.param("id"));
                                        } catch (Exception e) {
                                            return response_400.map(response);
                                        }

                                        Visit visit = visits.get(id);
                                        if (visit == null)
                                            return response.status(Status._404);

                                        try {
                                            if (!parseVisitUpdate(request, visit))
                                                return response_400.map(response);
                                        } catch (Exception e) {
                                            return response_400.map(response);
                                        }

                                        visit.staticResponse = prepareStaticJsonResponse(visit);
                                        return response
                                                .setBody("{}")
                                                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
                                    })
                    )
                    .append("/users/new", new RestHandler().post((request, response) -> {
//                        addCloseIfNotKeepAlive(request, response);
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
//                        addCloseIfNotKeepAlive(request, response);
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
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
                        } catch (Exception e) {
                            return response_400.map(response);
                        }
                    }))
                    .append("/visits/new", new RestHandler().post((request, response) -> {
//                        addCloseIfNotKeepAlive(request, response);
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
                                    .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
                        } catch (Exception e) {
                            return response_400.map(response);
                        }
                    }))
            ;
        });
    }

    public boolean parseUserUpdate(Request request, User user) {
        JsonObject update = JsonTools.parse(request.getBody().bytes()).asJsonObject();
        for (JsonItem item : update.values()) {
            if (item.isNull())
                return false;
        }

        for (Map.Entry<String, JsonItem> entry : update.entrySet()) {
            int hash = entry.getKey().hashCode();
            if ("email".hashCode() == hash) {
                user.email = entry.getValue().asString();
            } else if ("first_name".hashCode() == hash) {
                user.first_name = entry.getValue().asString();
            } else if ("last_name".hashCode() == hash) {
                user.last_name = entry.getValue().asString();
            } else if ("gender".hashCode() == hash) {
                user.gender = User.Gender.valueOf(entry.getValue().asString());
            } else if ("birth_date".hashCode() == hash) {
                user.birth_date = entry.getValue().asLong();
            }
        }

//        JsonItem item;
//        if ((item = update.get("email")) != null) {
//            user.email = item.asString();
//        }
//        if ((item = update.get("first_name")) != null) {
//            user.first_name = item.asString();
//        }
//        if ((item = update.get("last_name")) != null) {
//            user.last_name = item.asString();
//        }
//        if ((item = update.get("gender")) != null) {
//            user.gender = User.Gender.valueOf(item.asString());
//        }
//        if ((item = update.get("birth_date")) != null) {
//            user.birth_date = item.asLong();
//        }
        return true;
    }

    public boolean parseLocationUpdate(Request request, Location location) {
        JsonObject update = JsonTools.parse(request.getBody().bytes()).asJsonObject();
        for (JsonItem item : update.values()) {
            if (item.isNull())
                return false;
        }
        for (Map.Entry<String, JsonItem> entry : update.entrySet()) {
            int hash = entry.getKey().hashCode();
            if ("country".hashCode() == hash) {
                location.country = entry.getValue().asString();
            } else if ("city".hashCode() == hash) {
                location.city = entry.getValue().asString();
            } else if ("place".hashCode() == hash) {
                location.place = entry.getValue().asString();
            } else if ("distance".hashCode() == hash) {
                location.distance = entry.getValue().asInteger();
            }
        }
//        JsonItem item;
//        if ((item = update.get("country")) != null) {
//            location.country = item.asString();
//        }
//        if ((item = update.get("city")) != null) {
//            location.city = item.asString();
//        }
//        if ((item = update.get("place")) != null) {
//            location.place = item.asString();
//        }
//        if ((item = update.get("distance")) != null) {
//            location.distance = item.asInteger();
//        }
        return true;
    }

    public boolean parseVisitUpdate(Request request, Visit visit) {
        JsonObject update = JsonTools.parse(request.getBody().bytes()).asJsonObject();
        for (JsonItem item : update.values()) {
            if (item.isNull())
                return false;
        }

        for (Map.Entry<String, JsonItem> entry : update.entrySet()) {
            int hash = entry.getKey().hashCode();
            if ("user".hashCode() == hash) {
                removeFromVisitMaps(visit);
                visit.user = entry.getValue().asInteger();
                addToVisitMaps(visit);
            } else if ("location".hashCode() == hash) {
                removeFromVisitMaps(visit);
                visit.location = entry.getValue().asInteger();
                addToVisitMaps(visit);
            } else if ("mark".hashCode() == hash) {
                visit.mark = entry.getValue().asInteger();
            } else if ("visited_at".hashCode() == hash) {
                visit.visited_at = entry.getValue().asLong();
            }
        }
//        JsonItem item;
//        if ((item = update.get("user")) != null) {
//            removeFromVisitMaps(visit);
//            visit.user = item.asInteger();
//            addToVisitMaps(visit);
//        }
//        if ((item = update.get("location")) != null) {
//            removeFromVisitMaps(visit);
//            visit.location = item.asInteger();
//            addToVisitMaps(visit);
//        }
//        if ((item = update.get("mark")) != null) {
//            visit.mark = item.asInteger();
//        }
//        if ((item = update.get("visited_at")) != null) {
//            visit.visited_at = item.asLong();
//        }
        return true;
    }

    public Stream<VisitInfo> prepareLocationVisitsStream(Request request, Stream<VisitInfo> stream) {
        if (request.params().isEmpty())
            return stream;

        for (Map.Entry<String, MultiValue> entry : request.params().entrySet()) {
            String value = entry.getValue().value();
            int hash = entry.getKey().hashCode();
            if ("fromDate".hashCode() == hash) {
                long fromDate = Long.parseLong(value);
                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at > fromDate); //todo put into tree-map
            } else if ("toDate".hashCode() == hash) {
                long toDate = Long.parseLong(value);
                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at < toDate); //todo put into tree-map
            } else if ("fromAge".hashCode() == hash) {
                int fromAge = Integer.parseInt(value);
                long ageValue = getSecondsFromAge(fromAge);
                stream = stream.filter(visit -> visit.user.birth_date < ageValue);
            } else if ("toAge".hashCode() == hash) {
                int toAge = Integer.parseInt(value);
                long ageValue = getSecondsFromAge(toAge);
                stream = stream.filter(visit -> visit.user.birth_date > ageValue);
            } else if ("gender".hashCode() == hash) {
                User.Gender gender = User.Gender.valueOf(value);
                stream = stream.filter(visit -> visit.user.gender == gender);
            }
        }
//        String s;
//        s = request.param("fromDate");
//        if (s != null) {
//            long fromDate = Long.parseLong(s);
//            stream = stream.filter(visitInfo -> visitInfo.visit.visited_at > fromDate); //todo put into tree-map
//        }
//        s = request.param("toDate");
//        if (s != null) {
//            long toDate = Long.parseLong(s);
//            stream = stream.filter(visitInfo -> visitInfo.visit.visited_at < toDate); //todo put into tree-map
//        }
//        s = request.param("fromAge");
//        if (s != null) {
//            int fromAge = Integer.parseInt(s);
//            long ageValue = getSecondsFromAge(fromAge);
////                                long ageValue = System.currentTimeMillis() - fromAge * 1000l * 60 * 60 * 24 * 365;
//            stream = stream.filter(visit -> visit.user.birth_date < ageValue);
//        }
//        s = request.param("toAge");
//        if (s != null) {
//            int toAge = Integer.parseInt(s);
//            long ageValue = getSecondsFromAge(toAge);
////                                long ageValue = System.currentTimeMillis() - toAge * 1000l * 60 * 60 * 24 * 365;
//            stream = stream.filter(visit -> visit.user.birth_date > ageValue);
//        }
//        s = request.param("gender");
//        if (s != null) {
//            User.Gender gender = User.Gender.valueOf(s);
//            stream = stream.filter(visit -> visit.user.gender == gender);
//        }
        return stream;
    }

    public Stream<VisitInfo> prepareUserVisitsStream(Request request, Stream<VisitInfo> stream) {
        if (request.params().isEmpty())
            return stream;

        for (Map.Entry<String, MultiValue> entry : request.params().entrySet()) {
            String value = entry.getValue().value();
            int hash = entry.getKey().hashCode();
            if ("fromDate".hashCode() == hash) {
                long fromDate = Long.parseLong(value);
                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at > fromDate); //todo put into tree-map
            } else if ("toDate".hashCode() == hash) {
                long toDate = Long.parseLong(value);
                stream = stream.filter(visitInfo -> visitInfo.visit.visited_at < toDate); //todo put into tree-map
            } else if ("toDistance".hashCode() == hash) {
                int toDistance = Integer.parseInt(value);
                stream = stream.filter(visitInfo -> visitInfo.location.distance < toDistance);
            } else if ("country".hashCode() == hash) {
                stream = stream.filter(visitInfo -> visitInfo.location.country.hashCode() == value.hashCode() && value.equals(visitInfo.location.country));
            }
        }
//        String s;
//        s = request.param("fromDate");
//        if (s != null) {
//            long fromDate = Long.parseLong(s);
//            stream = stream.filter(visitInfo -> visitInfo.visit.visited_at > fromDate); //todo put into tree-map
//        }
//        s = request.param("toDate");
//        if (s != null) {
//            long toDate = Long.parseLong(s);
//            stream = stream.filter(visitInfo -> visitInfo.visit.visited_at < toDate); //todo put into tree-map
//        }
//        s = request.param("toDistance");
//        if (s != null) {
//            int toDistance = Integer.parseInt(s);
//            stream = stream.filter(visitInfo -> visitInfo.location.distance < toDistance);
//        }
//        s = request.param("country");
//        if (s != null) {
//            String country = s;
//            stream = stream.filter(visitInfo -> country.equals(visitInfo.location.country));
//        }
        return stream;
    }

    public List<VisitInfo> getMaxVisitsByLocation() {
        return visitsByLocation.values().stream().max(Comparator.comparingInt(List::size)).orElse(Collections.emptyList());
    }

    public List<VisitInfo> getMaxVisitsByUser() {
        return visitsByUser.values().stream().max(Comparator.comparingInt(List::size)).orElse(Collections.emptyList());
    }

    public byte[] serializeJson(Object data) {
        return JsonTools.serializeToBytes(data);
    }

    public void addCloseIfNotKeepAlive(com.wizzardo.http.request.Request request, Response response) {
//        if (!request.connection().isKeepAlive())
//            response.appendHeader(Header.KV_CONNECTION_CLOSE);
    }

    public void removeFromVisitMaps(Visit visit) {
        visitsByUser.get(visit.user).removeIf(visitInfo -> visitInfo.visit.id == visit.id);
        visitsByLocation.get(visit.location).removeIf(visitInfo -> visitInfo.visit.id == visit.id);
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
//        System.out.println(Unchecked.ignore(() -> exec("/opt/wrk -v"), "wrk failed to start"));

        time = System.currentTimeMillis();
        if (!app.users.isEmpty())
            if (app.dataLength / 1024 / 1024 < 10)
                warmUp(app, 22 - startupTime, 2000);
            else
                warmUp(app, 165 - startupTime, 3000);

        System.out.println("warmUp finished in " + (System.currentTimeMillis() - time) / 1000f + " seconds");
        System.gc();
    }

    public static String formatBytes(long bytes) {
        return bytes / 1024 / 1024 + " mb";
    }

    private static void warmUp(App app, float seconds, long pause) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int u;
        while (!app.users.containsKey(u = random.nextInt(app.users.size()))) {
        }

        int l;
        while (!app.locations.containsKey(l = random.nextInt(app.locations.size()))) {
        }
//        int user = app.getMaxVisitsByUser().get(0).user.id;
//        int location = app.getMaxVisitsByLocation().get(0).location.id;
        long time = System.currentTimeMillis();
        int user = u;
        int location = l;
        while ((System.currentTimeMillis() - time) / 1000 < seconds) {
            String exec;
//            exec = Unchecked.ignore(() -> exec("ab -c 2 -k -n 10000 http://127.0.0.1:" + app.getPort() + "/users/" + user + "/visits"), "");
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 1 -d 1 http://127.0.0.1:" + app.getPort() + "/users/" + user + "/visits"), "");
//            System.out.println("visits by user: " + TextTools.find(exec, Pattern.compile("Requests per second:\\s+\\d+.\\d+")));
            System.out.println("visits by user: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));
//            exec = Unchecked.ignore(() -> exec("ab -c 2 -k -n 10000 http://127.0.0.1:" + app.getPort() + "/locations/" + location + "/avg"), "");
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 1 -d 1 http://127.0.0.1:" + app.getPort() + "/locations/" + location + "/avg"), "");
//            System.out.println("locations avg: " + TextTools.find(exec, Pattern.compile("Requests per second:\\s+\\d+.\\d+")));
            System.out.println("locations avg: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));
//            exec = Unchecked.ignore(() -> exec("ab -c 2 -k -n 10000 http://127.0.0.1:" + app.getPort() + "/users/" + user), "");
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 1 -d 1 http://127.0.0.1:" + app.getPort() + "/users/" + user), "");
//            System.out.println("user by id: " + TextTools.find(exec, Pattern.compile("Requests per second:\\s+\\d+.\\d+")));
            System.out.println("user by id: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));
//            System.out.println(exec);

            FileTools.text("/tmp/post.lua", "" +
                    "wrk.method = \"POST\"\n" +
                    "wrk.body   = \"" + JsonTools.serialize(app.users.get(user)).replace('"', '\'') + "\"\n" +
                    "wrk.headers[\"Content-Type\"] = \"application/json\"" +
                    "");
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 1 -d 1 -s /tmp/post.lua http://127.0.0.1:" + app.getPort() + "/users/" + user), "");
            System.out.println("post user update: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            Unchecked.ignore(() -> Thread.sleep(pause));
        }
    }

    private void initData() {
        File zipFile = new File("/tmp/data/data.zip");

        Locations locations = new Locations();
        Users users = new Users();
        Visits visits = new Visits();
        AtomicLong sizeCounter = new AtomicLong();
        Unchecked.run(() -> {
            if (!zipFile.exists())
                return;

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
//                System.out.println(stopwatch);
            }
            IOTools.close(zip);
        });
        System.out.println("total jsons size: " + sizeCounter.get() / 1024 / 1024 + " MB");

        this.dataLength = sizeCounter.get();
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
//                .appendHeader(Header.KV_CONNECTION_CLOSE)
                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
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
