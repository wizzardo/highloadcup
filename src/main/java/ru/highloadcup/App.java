package ru.highloadcup;

import com.wizzardo.epoll.readable.ReadableByteArray;
import com.wizzardo.epoll.readable.ReadableByteBuffer;
import com.wizzardo.http.*;
import com.wizzardo.http.mapping.Path;
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
import com.wizzardo.tools.misc.pool.Holder;
import com.wizzardo.tools.misc.pool.Pool;
import com.wizzardo.tools.misc.pool.PoolBuilder;
import com.wizzardo.tools.misc.pool.SimpleHolder;
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
public class App extends HttpServer<HttpConnection> {
    protected static final byte[] HEADER_SERVER_NAME = "Server: wizzardo-http/0.1\r\n".getBytes();
    protected static final byte[] RESPONSE_EMPTY_VISITS = {'{', '"', 'v', 'i', 's', 'i', 't', 's', '"', ':', '[', ']', '}'};

    public static final Mapper<Response, Response> response_400;
    public static final Mapper<Response, Response> response_404;
    public static final Mapper<Response, Response> response_200;

    static {
        ServerDate serverDate = new ServerDate();

        ReadableByteBuffer static_400 = new Response()
                .status(Status._400)
                .setBody("")
                .appendHeader(serverDate.getDateAsBytes())
                .appendHeader(HEADER_SERVER_NAME)
                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                .appendHeader(Header.KV_CONNECTION_CLOSE)
                .buildStaticResponse();
        ReadableByteBuffer static_404 = new Response()
                .status(Status._404)
                .setBody("")
                .appendHeader(serverDate.getDateAsBytes())
                .appendHeader(HEADER_SERVER_NAME)
                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                .appendHeader(Header.KV_CONNECTION_CLOSE)
                .buildStaticResponse();
        ReadableByteBuffer static_200 = new Response()
                .setBody("{}")
                .appendHeader(HEADER_SERVER_NAME)
                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                .appendHeader(Header.KV_CONNECTION_CLOSE)
                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                .buildStaticResponse();
        response_400 = (response) -> response.setStaticResponse(static_400.copy());
        response_404 = (response) -> response.setStaticResponse(static_404.copy());
        response_200 = (response) -> response.setStaticResponse(static_200.copy());
    }

    Map<Integer, User> users;
    Map<Integer, Location> locations;
    Map<Integer, Visit> visits;

    Map<Integer, List<VisitInfo>> visitsByUser;
    Map<Integer, List<VisitInfo>> visitsByLocation;
    long[] years;

    long dataLength;
    Pool<byte[]> byteArrayPool = new PoolBuilder<byte[]>()
            .queue(PoolBuilder.createThreadLocalQueueSupplier())
            .supplier(() -> new byte[10240])
            .holder(SimpleHolder::new)
            .build();

    public App(String[] args) {
        int port = 8080;
        if (args.length == 1 && args[0].equals("env=prod"))
            port = 80;

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

        App a = this;

        a.setPort(port);
        a.setIoThreadsCount(0);

        a.getUrlMapping()
                .append("/users/$id/visits", new RestHandler().get((request, response) -> getVisitsByUser(request.param("id"), request, response)))
                .append("/locations/$id/avg", new RestHandler().get((request, response) -> getLocationAverageMark(request.param("id"), request, response)))
                .append("/users/$id", new RestHandler()
                        .get((request, response) -> getUserById(request.param("id"), request, response))
                        .post((request, response) -> updateUser(request.param("id"), request, response))
                )
                .append("/locations/$id", new RestHandler()
                        .get((request, response) -> getLocationById(request.param("id"), request, response))
                        .post((request, response) -> updateLocation(request.param("id"), request, response))
                )
                .append("/visits/$id", new RestHandler()
                        .get((request, response) -> getVisitById(request.param("id"), request, response))
                        .post((request, response) -> updateVisit(request.param("id"), request, response))
                )
                .append("/users/new", new RestHandler().post(this::newUser))
                .append("/locations/new", new RestHandler().post(this::newLocation))
                .append("/visits/new", new RestHandler().post(this::newVisit))
        ;
    }

    @Override
    protected void handle(HttpConnection connection) throws Exception {
        Request request = connection.getRequest();
        Response response = connection.getResponse();

        connection.setKeepAlive(true);

        handle(request, response);
    }

    @Override
    protected Response handle(Request request, Response response) throws IOException {
        Path path = request.path();
        if (path.length() == 2) {
            if ("users".hashCode() == path.getPart(0).hashCode()) {
                if (request.method() == Request.Method.GET) {
                    response = getUserById(path.getPart(1), request, response);
                } else {
                    if ("new".hashCode() == path.getPart(1).hashCode())
                        response = newUser(request, response);
                    else
                        response = updateUser(path.getPart(1), request, response);
                }
            } else if ("locations".hashCode() == path.getPart(0).hashCode()) {
                if (request.method() == Request.Method.GET) {
                    response = getLocationById(path.getPart(1), request, response);
                } else {
                    if ("new".hashCode() == path.getPart(1).hashCode())
                        response = newLocation(request, response);
                    else
                        response = updateLocation(path.getPart(1), request, response);
                }
            } else if ("visits".hashCode() == path.getPart(0).hashCode()) {
                if (request.method() == Request.Method.GET) {
                    response = getVisitById(path.getPart(1), request, response);
                } else {
                    if ("new".hashCode() == path.getPart(1).hashCode())
                        response = newVisit(request, response);
                    else
                        response = updateVisit(path.getPart(1), request, response);
                }
            }
        } else if (path.length() == 3) {
            if ("users".hashCode() == path.getPart(0).hashCode()) {
                response = getVisitsByUser(path.getPart(1), request, response);
            } else if ("locations".hashCode() == path.getPart(0).hashCode()) {
                response = getLocationAverageMark(path.getPart(1), request, response);
            }
        } else
            response_404.map(response);
        return response;
    }

    protected Response updateUser(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_404.map(response);
        }

        User user = users.get(id);
        if (user == null)
            return response_404.map(response);

        try {
            if (!parseUserUpdate(request, user))
                return response_400.map(response);
        } catch (Exception e) {
            return response_400.map(response);
        }

        user.staticResponse = prepareStaticJsonResponse(user);
        return response_200.map(response);
    }

    protected Response updateLocation(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_404.map(response);
        }

        Location location = locations.get(id);
        if (location == null)
            return response_404.map(response);

        try {
            if (!parseLocationUpdate(request, location))
                return response_400.map(response);
        } catch (Exception e) {
            return response_400.map(response);
        }

        location.staticResponse = prepareStaticJsonResponse(location);
        return response_200.map(response);
    }

    protected Response updateVisit(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_400.map(response);
        }

        Visit visit = visits.get(id);
        if (visit == null)
            return response_404.map(response);

        try {
            if (!parseVisitUpdate(request, visit))
                return response_400.map(response);
        } catch (Exception e) {
            return response_400.map(response);
        }

        visit.staticResponse = prepareStaticJsonResponse(visit);
        return response_200.map(response);
    }

    protected Response getUserById(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_404.map(response);
        }

        User user = users.get(id);
        if (user == null)
            return response_404.map(response);

        return response.setStaticResponse(user.staticResponse.copy());
    }

    protected Response getLocationById(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_404.map(response);
        }

        Location location = locations.get(id);
        if (location == null)
            return response_404.map(response);

        return response.setStaticResponse(location.staticResponse.copy());
    }

    protected Response getVisitById(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_400.map(response);
        }
        Visit visit = visits.get(id);
        if (visit == null)
            return response_404.map(response);

        return response.setStaticResponse(visit.staticResponse.copy());
    }

    protected Response newUser(Request request, Response response) {
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
            return response_200.map(response);
        } catch (Exception e) {
            return response_400.map(response);
        }
    }

    protected Response newLocation(Request request, Response response) {
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
            return response_200.map(response);
        } catch (Exception e) {
            return response_400.map(response);
        }
    }

    protected Response newVisit(Request request, Response response) {
        try {
            Visit update = JsonTools.parse(request.getBody().bytes(), Visit.class);
            Visit visit = visits.get(update.id);
            if (visit != null)
                return response_400.map(response);

            visits.put(update.id, update);
            addToVisitMaps(update);
            update.staticResponse = prepareStaticJsonResponse(update);
            return response_200.map(response);
        } catch (Exception e) {
            return response_400.map(response);
        }
    }

    protected Response getLocationAverageMark(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_404.map(response);
        }

        List<VisitInfo> visitList = visitsByLocation.get(id);
        if (visitList == null)
            return response_404.map(response);

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

        byte[] s = new ExceptionDrivenStringBuilder().append("{\"avg\":").append(result).append("}").toBytes();
        return response
                .setBody(s)
                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                            .appendHeader(Header.KV_CONNECTION_CLOSE)
                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
    }

    protected Response getVisitsByUser(String idString, Request request, Response response) {
        int id;
        try {
            id = Integer.parseInt(idString);
        } catch (Exception e) {
            return response_404.map(response);
        }

        List<VisitInfo> visitList = visitsByUser.get(id);
        if (visitList == null)
            return response_404.map(response);

        Stream<VisitInfo> stream = visitList.stream();
        try {
            stream = prepareUserVisitsStream(request, stream);
        } catch (Exception e) {
            return response_400.map(response);
        }

        List<VisitInfo> results = stream.sorted(Comparator.comparingLong(o -> o.visit.visited_at)) //todo make collection sorted by default
                .collect(Collectors.toList());

        byte[] result;
        if (results.isEmpty()) {
            result = RESPONSE_EMPTY_VISITS;
            response.setBody(result);
        } else {
            Holder<byte[]> holder = byteArrayPool.holder();
            result = holder.get();
//                            int size = 1 + 2 + 2 + 1 + 6; // {"visits":[ + ] - last ',' }
//                            for (VisitInfo vi : results) {
//                                size += vi.getJson().length + 1;
//                            }
//                            result = new byte[size];
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
                byte[] json = vi.getJson();
                System.arraycopy(json, 0, result, offset, json.length);
                offset += json.length;
                result[offset++] = ',';
            }
            result[offset - 1] = ']';
            result[offset] = '}';

            response.setBody(new ReadableByteArray(result, 0, offset + 1) {
                @Override
                public void close() {
                    holder.close();
                }
            });
        }

        return response
                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                            .appendHeader(Header.KV_CONNECTION_CLOSE)
                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON);
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
        long time = System.currentTimeMillis();
        int user = u;
        int location = l;
        while ((System.currentTimeMillis() - time) / 1000 < seconds) {
            String exec;
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 1 -d 1 http://127.0.0.1:" + app.getPort() + "/users/" + user + "/visits"), "");
            System.out.println("visits by user: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 1 -d 1 http://127.0.0.1:" + app.getPort() + "/locations/" + location + "/avg"), "");
            System.out.println("locations avg: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 1 -d 1 http://127.0.0.1:" + app.getPort() + "/users/" + user), "");
            System.out.println("user by id: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

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
                sizeCounter.addAndGet(nextEntry.getSize());
                IOTools.copy(zip, out, buffer);
                String json = out.toString();
                out.reset();

                if (name.startsWith("locations"))
                    locations.locations.addAll(JsonTools.parse(json, Locations.class).locations);
                else if (name.startsWith("users"))
                    users.users.addAll(JsonTools.parse(json, Users.class).users);
                else if (name.startsWith("visits"))
                    visits.visits.addAll(JsonTools.parse(json, Visits.class).visits);
            }
            IOTools.close(zip);
        });
        System.out.println("total jsons size: " + sizeCounter.get() / 1024 / 1024 + " MB");

        this.dataLength = sizeCounter.get();
        this.users = initMap(users.users);
        this.locations = initMap(locations.locations);
        this.visits = initMap(visits.visits);

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
        Map<Integer, T> map = new ConcurrentHashMap<>(data.size() + 1, 1f);
        for (T t : data) {
            map.put(t.id(), t);
        }
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
