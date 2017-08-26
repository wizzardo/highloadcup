package ru.highloadcup;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.wizzardo.epoll.ByteBufferProvider;
import com.wizzardo.epoll.readable.ReadableByteArray;
import com.wizzardo.epoll.readable.ReadableByteBuffer;
import com.wizzardo.epoll.readable.ReadableData;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class App extends HttpServer<App.MeasuredHttpConnection> {
    protected static final byte[] HEADER_SERVER_NAME = "Server: wizzardo-http/0.1\r\n".getBytes();
    protected static final byte[] RESPONSE_EMPTY_VISITS = {'{', '"', 'v', 'i', 's', 'i', 't', 's', '"', ':', '[', ']', '}'};

    static Pool<byte[]> byteArrayPool = new PoolBuilder<byte[]>()
//            .queue(LinkedList::new)
            .queue(PoolBuilder.createThreadLocalQueueSupplier())
            .supplier(() -> {
                System.out.println("new byte array");
                return new byte[1024 * 1024];
            })
            .holder(SimpleHolder::new)
            .build();

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
//                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                .buildStaticResponse();
        response_400 = (response) -> response.setStaticResponse(static_400.copy());
        response_404 = (response) -> response.setStaticResponse(static_404.copy());
        response_200 = (response) -> response.setStaticResponse(static_200.copy());
    }

    HashIntObjMap<User> users;
    HashIntObjMap<Location> locations;
    HashIntObjMap<Visit> visits;

    HashIntObjMap<ArrayList<VisitInfo>> visitsByUser;
    HashIntObjMap<ArrayList<VisitInfo>> visitsByLocation;
    long[] years;

    long dataLength;

    public static AtomicLong handleTimeCounter = new AtomicLong();
    public static AtomicLong writeTimeCounter = new AtomicLong();
    public static AtomicLong readTimeCounter = new AtomicLong();

    public static AtomicLong userGetTimeCounter = new AtomicLong();
    public static AtomicLong userUpdateTimeCounter = new AtomicLong();
    public static AtomicLong userNewTimeCounter = new AtomicLong();
    public static AtomicLong locationGetTimeCounter = new AtomicLong();
    public static AtomicLong locationUpdateTimeCounter = new AtomicLong();
    public static AtomicLong locationNewTimeCounter = new AtomicLong();
    public static AtomicLong visitGetTimeCounter = new AtomicLong();
    public static AtomicLong visitUpdateTimeCounter = new AtomicLong();
    public static AtomicLong visitNewTimeCounter = new AtomicLong();
    public static AtomicLong visitsTimeCounter = new AtomicLong();
    public static AtomicLong locationsTimeCounter = new AtomicLong();

    public static AtomicLong connectionsCounter = new AtomicLong();
    public static AtomicLong requestTimeCounter = new AtomicLong();
    public static AtomicLong requestCounter = new AtomicLong();
    public static ConcurrentHashMap<Integer, Integer> uniqueConnections = new ConcurrentHashMap<>();

    LinkedList<MeasuredHttpConnection> preparedConnections = new LinkedList<>();

    private void initConnections(int n) {
        for (int i = 0; i < n; i++) {
            preparedConnections.add(new MeasuredHttpConnection(-1, -1, -1, this));
        }
    }

    static class MeasuredHttpConnection extends HttpConnection {
        long start;
        boolean handled = false;

        public MeasuredHttpConnection(int fd, int ip, int port, AbstractHttpServer server) {
            super(fd, ip, port, server);
        }

        @Override
        public boolean check(ByteBuffer bb) {
            if (start == -1)
                start = System.nanoTime();

            return super.check(bb);
        }

        @Override
        public boolean write(ByteBufferProvider bufferProvider) {
            try {
                return super.write(bufferProvider);
            } finally {
                if (handled) {
                    requestCounter.incrementAndGet();
                    requestTimeCounter.addAndGet(System.nanoTime() - start);
                    start = -1;
                    handled = false;
                }
            }
        }

        @Override
        protected boolean prepareKeepAlive() {
            return true;
        }

        @Override
        protected boolean processInputListener() {
            return false;
        }

        @Override
        protected boolean processOutputListener() {
            return false;
        }

        @Override
        public boolean isAlive() {
            return true;
        }

        @Override
        public void onWriteData(ReadableData readable, boolean hasMore) {
        }

        @Override
        protected boolean actualWrite(ReadableData readable, ByteBufferProvider bufferProvider) throws IOException {
            long time = System.nanoTime();
            try {
                return super.actualWrite(readable, bufferProvider);
            } finally {
                writeTimeCounter.addAndGet(System.nanoTime() - time);
            }
        }

        public ByteBuffer read(int length, ByteBufferProvider bufferProvider) throws IOException {
            long time = System.nanoTime();
            try {
                return super.read(length, bufferProvider);
            } finally {
                readTimeCounter.addAndGet(System.nanoTime() - time);
            }
        }
    }

    @Override
    protected MeasuredHttpConnection createConnection(int fd, int ip, int port) {
        connectionsCounter.incrementAndGet();
        uniqueConnections.put(fd, fd);
        MeasuredHttpConnection preparedConnection = preparedConnections.poll();
        if (preparedConnection == null)
            preparedConnection = new MeasuredHttpConnection(fd, ip, port, this);
        else {
            preparedConnection.init(fd, ip, port);
        }
        return preparedConnection;
//        return super.createConnection(fd, ip, port);
//        return new MeasuredHttpConnection(fd, ip, port, this);
    }

    public App(String[] args) {
        //todo change write queue if only 1 thread to linkedlist
        //todo replace streams with manual checks
        //todo make flow cacheable

        int port = 8080;
        if (args.length == 1 && args[0].equals("env=prod"))
            port = 80;

//        Stopwatch stopwatch = new Stopwatch("initialized in");
        initData();
//        System.out.println(stopwatch);
//        System.out.println("users: " + users.size());
//        System.out.println("locations: " + locations.size());
//        System.out.println("visits: " + visits.size());
//        System.out.println("visits by user: " + visitsByUser.size());
//        System.out.println("visits by location: " + visitsByLocation.size());
//        List<VisitInfo> maxVisitsByUser = getMaxVisitsByUser();
//        if (!maxVisitsByUser.isEmpty())
//            System.out.println("max visits by user." + maxVisitsByUser.get(0).user.id + ": " + maxVisitsByUser.size());
//
//        List<VisitInfo> maxVisitsByLocation = getMaxVisitsByLocation();
//        if (!maxVisitsByLocation.isEmpty())
//            System.out.println("max visits by location." + maxVisitsByLocation.get(0).location.id + ": " + maxVisitsByLocation.size());

        App a = this;

        a.setPort(port);
//        a.setIoThreadsCount(Runtime.getRuntime().availableProcessors());

        //change pool queue if >0
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
    protected void handle(MeasuredHttpConnection connection) throws Exception {
        long time = System.nanoTime();

        try {
            handle(connection.getRequest(), connection.getResponse());
        } finally {
            connection.handled = true;
            handleTimeCounter.addAndGet(System.nanoTime() - time);
        }
    }

    @Override
    protected Response handle(Request request, Response response) throws IOException {
        long time = System.nanoTime();
        Path path = request.path();
        if (path.length() == 2) {
            if ("users".hashCode() == path.getPart(0).hashCode()) {
                if (request.method() == Request.Method.GET) {
                    response = getUserById(path.getPart(1), request, response);
                    userGetTimeCounter.addAndGet(System.nanoTime() - time);
                } else {
                    if ("new".hashCode() == path.getPart(1).hashCode()) {
                        response = newUser(request, response);
                        userNewTimeCounter.addAndGet(System.nanoTime() - time);
                    } else {
                        response = updateUser(path.getPart(1), request, response);
                        userUpdateTimeCounter.addAndGet(System.nanoTime() - time);
                    }
                }
            } else if ("locations".hashCode() == path.getPart(0).hashCode()) {
                if (request.method() == Request.Method.GET) {
                    response = getLocationById(path.getPart(1), request, response);
                    locationGetTimeCounter.addAndGet(System.nanoTime() - time);
                } else {
                    if ("new".hashCode() == path.getPart(1).hashCode()) {
                        response = newLocation(request, response);
                        locationNewTimeCounter.addAndGet(System.nanoTime() - time);
                    } else {
                        response = updateLocation(path.getPart(1), request, response);
                        locationUpdateTimeCounter.addAndGet(System.nanoTime() - time);
                    }
                }
            } else if ("visits".hashCode() == path.getPart(0).hashCode()) {
                if (request.method() == Request.Method.GET) {
                    response = getVisitById(path.getPart(1), request, response);
                    visitGetTimeCounter.addAndGet(System.nanoTime() - time);
                } else {
                    if ("new".hashCode() == path.getPart(1).hashCode()) {
                        response = newVisit(request, response);
                        visitNewTimeCounter.addAndGet(System.nanoTime() - time);
                    } else {
                        response = updateVisit(path.getPart(1), request, response);
                        visitUpdateTimeCounter.addAndGet(System.nanoTime() - time);
                    }
                }
            }
        } else if (path.length() == 3) {
            if ("users".hashCode() == path.getPart(0).hashCode()) {
                response = getVisitsByUser(path.getPart(1), request, response);
                visitsTimeCounter.addAndGet(System.nanoTime() - time);
            } else if ("locations".hashCode() == path.getPart(0).hashCode()) {
                response = getLocationAverageMark(path.getPart(1), request, response);
                locationsTimeCounter.addAndGet(System.nanoTime() - time);
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

//        visit.staticResponse = prepareStaticJsonResponse(visit);
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

//        return response.setStaticResponse(visit.staticResponse.copy());
        return response.setBody(JsonTools.serializeToBytes(visit));
    }

    protected Response newUser(Request request, Response response) {
        try {
            UserUpdate update = JsonTools.parse(request.getBody().bytes(), UserUpdate.class);
            if (update.id == null || update.birth_date == null || update.first_name == null
                    || update.last_name == null || update.email == null || update.gender == null)
                return response_400.map(response);

            int id = update.id;
            User user = users.get(id);
            if (user != null)
                return response_400.map(response);

            user = new User();
            user.birth_date = update.birth_date;
            user.first_name = update.first_name;
            user.last_name = update.last_name;
            user.id = id;
            user.gender = update.gender;
            user.email = update.email;

            users.put(id, user);
            visitsByUser.computeIfAbsent(id, integer -> new ArrayList<>());
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

            int id = update.id;
            Location location = locations.get(id);
            if (location != null)
                return response_400.map(response);

            location = new Location();
            location.id = id;
            location.distance = update.distance;
            location.city = update.city;
            location.country = update.country;
            location.place = update.place;
            locations.put(id, location);
            visitsByLocation.computeIfAbsent(id, integer -> new ArrayList<>());
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
//            update.staticResponse = prepareStaticJsonResponse(update);
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
//                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                            .appendHeader(Header.KV_CONNECTION_CLOSE)
//                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                ;
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


        VisitsReducer reducer = new VisitsReducer();
        VisitInfo reduce = stream
//                .map(VisitInfo::getJson)
                .reduce(null, reducer);
        if (reduce == null) {
            response.setBody(RESPONSE_EMPTY_VISITS);
        } else {
            response.setBody(reducer.toReadableByteArray());
        }

//        List<VisitInfo> results = stream
////                .sorted(Comparator.comparingLong(o -> o.visit.visited_at))
//                .collect(Collectors.toList());
//
//        byte[] result;
//        if (results.isEmpty()) {
//            result = RESPONSE_EMPTY_VISITS;
//            response.setBody(result);
//        } else {
//            Holder<byte[]> holder = byteArrayPool.holder();
//            result = holder.get();
////                            int size = 1 + 2 + 2 + 1 + 6; // {"visits":[ + ] - last ',' }
////                            for (VisitInfo vi : results) {
////                                size += vi.getJson().length + 1;
////                            }
////                            result = new byte[size];
//            result[0] = '{';
//            result[1] = '"';
//            result[2] = 'v';
//            result[3] = 'i';
//            result[4] = 's';
//            result[5] = 'i';
//            result[6] = 't';
//            result[7] = 's';
//            result[8] = '"';
//            result[9] = ':';
//            result[10] = '[';
//            int offset = 11;
//            for (VisitInfo vi : results) {
//                byte[] json = vi.getJson();
//                System.arraycopy(json, 0, result, offset, json.length);
//                offset += json.length;
//                result[offset++] = ',';
//            }
//            result[offset - 1] = ']';
//            result[offset] = '}';
//
//            response.setBody(new ReadableByteArray(result, 0, offset + 1) {
//                @Override
//                public void close() {
//                    holder.close();
//                }
//            });
//        }

        return response
//                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                            .appendHeader(Header.KV_CONNECTION_CLOSE)
//                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                ;
    }

    public boolean parseUserUpdate(Request request, User user) {
        JsonObject update = JsonTools.parse(request.getBody().bytes()).asJsonObject();
        for (Map.Entry<String, JsonItem> item : update.entrySet()) {
            if (item.getValue().isNull())
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
        for (Map.Entry<String, JsonItem> item : update.entrySet()) {
            if (item.getValue().isNull())
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
        for (Map.Entry<String, JsonItem> item : update.entrySet()) {
            if (item.getValue().isNull())
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
                sortVisits(visitsByUser.get(visit.user));
            }
        }
        return true;
    }

    public Stream<VisitInfo> prepareLocationVisitsStream(Request request, Stream<VisitInfo> stream) {
        if (!request.hasParameters())
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
        if (!request.hasParameters())
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

//    public List<VisitInfo> getMaxVisitsByLocation() {
//        return visitsByLocation.values().stream().max(Comparator.comparingInt(List::size)).orElse(Collections.emptyList());
//    }
//
//    public List<VisitInfo> getMaxVisitsByUser() {
//        return visitsByUser.values().stream().max(Comparator.comparingInt(List::size)).orElse(Collections.emptyList());
//    }

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
        visitsByLocation.computeIfAbsent(visit.location, integer -> new ArrayList<>(32))
                .add(createVisitInfo(visit));
    }

    public VisitInfo createVisitInfo(Visit visit) {
        return new VisitInfo(visit, locations.get(visit.location), users.get(visit.user));
    }

    public void addToVisitsByUser(Visit visit) {
        List<VisitInfo> visitInfos = visitsByUser.computeIfAbsent(visit.user, integer -> new ArrayList<>(32));
        visitInfos.add(createVisitInfo(visit));
        sortVisits(visitInfos);
    }

    public void sortVisits(List<VisitInfo> visitInfos) {
        visitInfos.sort(Comparator.comparingLong(o -> o.visit.visited_at));
    }

    public long getSecondsFromAge(int age) {
        if (age < years.length)
            return years[age];
        else
            return getSecondsOfAge(age);
    }

    static String ip;

    public static void main(String[] args) {
        Monitoring.initSystemMonitoring();
//        ip = TextTools.find(exec("ip addr show eth0"), Pattern.compile("inet (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})"), 1);
        ip = "localhost";


////        System.out.println("uname: " + exec("uname -r"));
//        System.out.println("tcp_low_latency: " + exec("echo 1 > /proc/sys/net/ipv4/tcp_low_latency"));
//        System.out.println("tcp_low_latency: " + exec("cat /proc/sys/net/ipv4/tcp_low_latency"));
//        System.out.println("tcp_fastopen: " + exec("echo 2 > /proc/sys/net/ipv4/tcp_fastopen"));
//        System.out.println("tcp_sack: " + exec("echo 0 > /proc/sys/net/ipv4/tcp_sack"));
//        System.out.println("tcp_timestamps: " + exec("echo 0 > /proc/sys/net/ipv4/tcp_timestamps"));
////        System.out.println("net.ipv4.tcp_rmem: " + exec("sysctl net.ipv4.tcp_rmem"));
//        System.out.println("tcp_no_metrics_save: " + exec("echo 1 > /proc/sys/net/ipv4/tcp_no_metrics_save"));
////        System.out.println("sched_rt_runtime_us: " + exec("echo -1 > /proc/sys/kernel/sched_rt_runtime_us"));

        long time = System.currentTimeMillis();
        App app = new App(args);
//        app.setHostname(ip);
        app.start();
        float startupTime = (System.currentTimeMillis() - time) / 1000f;
        System.out.println("App started in " + startupTime + " seconds");
//        bindIoThreadToCpu();
//        System.out.println(Unchecked.ignore(() -> exec("/opt/wrk -v"), "wrk failed to start"));
//        bindIoThreadToCpu();
        time = System.currentTimeMillis();
        if (!app.users.isEmpty() && args.length > 0)
            if (app.dataLength / 1024 / 1024 < 20)
                warmUp(app, 60 - startupTime - 10, 2000);
            else
                warmUp(app, 600 - startupTime - 60, 5000);


        app.initConnections(10000);
        System.out.println("warmUp finished in " + (System.currentTimeMillis() - time) / 1000f + " seconds. Connections openned: " + connectionsCounter.get() + " total response time: " + requestTimeCounter.get() / 1000 / 1000 + " ms, requests made: " + requestCounter.get());
        connectionsCounter.set(0);
        requestTimeCounter.set(0);
        requestCounter.set(0);
        handleTimeCounter.set(0);
        writeTimeCounter.set(0);
        readTimeCounter.set(0);
        uniqueConnections.clear();

        userGetTimeCounter.set(0);
        userNewTimeCounter.set(0);
        userUpdateTimeCounter.set(0);
        visitGetTimeCounter.set(0);
        visitNewTimeCounter.set(0);
        visitUpdateTimeCounter.set(0);
        locationGetTimeCounter.set(0);
        locationNewTimeCounter.set(0);
        locationUpdateTimeCounter.set(0);

        locationsTimeCounter.set(0);
        visitsTimeCounter.set(0);
        System.gc();
    }

    private static void bindIoThreadToCpu() {
        String exec = exec("ps -aux");
        exec = Arrays.stream(exec.split("\n")).filter(s -> s.contains("solution.jar") || s.contains("solution-all-1.0-SNAPSHOT.jar")).findFirst().get();
        System.out.println(exec);
        String[] split = exec.split("\\s+");
        String pid = split[1];
        System.out.println("pid: " + pid);
        exec = exec("jstack " + pid);
//        System.out.println(exec);
        for (int i = 0; i < Math.min(Runtime.getRuntime().availableProcessors(), 4); i++) {
            bindIoThreadToCpu(i, exec);
        }
//        exec = Arrays.stream(exec.split("\n")).filter(s -> s.contains("Thread-0")).findFirst().get();
//        System.out.println(exec);
//        String threadID = TextTools.find(exec, Pattern.compile("nid=0x([0-9a-fA-F]+)"), 1);
//        System.out.println("threadID: " + threadID + " " + Integer.parseInt(threadID, 16));
//        exec = exec("taskset -p -c 0 " + Integer.parseInt(threadID, 16));
//        System.out.println(exec);
//        exec = exec("chrt -f -p 20 " + Integer.parseInt(threadID, 16));
//        System.out.println(exec);
    }

    private static void bindIoThreadToCpu(int i, String jstack) {
        String exec = Arrays.stream(jstack.split("\n")).filter(s -> s.contains("IOThread-" + i)).findFirst().get();
        String threadID = TextTools.find(exec, Pattern.compile("nid=0x([0-9a-fA-F]+)"), 1);
        System.out.println("threadID: " + threadID + " " + Integer.parseInt(threadID, 16));
        exec = exec("taskset -p -c " + i + " " + Integer.parseInt(threadID, 16));
        System.out.println(exec);
    }

    private static void warmUp(App app, float seconds, long pause) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int u;
        while (!app.users.containsKey(u = random.nextInt(app.users.size()))) {
        }

        int l;
        while (app.locations.get(l = random.nextInt(app.locations.size())) == null) {
        }

        int v;
        while (app.visits.get(v = random.nextInt(app.visits.size())) == null) {
        }
        long time = System.currentTimeMillis();
        int user = u;
        int location = l;
        int visit = v;
        while ((System.currentTimeMillis() - time) / 1000 < seconds) {
            String exec;
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 http://" + ip + ":" + app.getPort() + "/users/" + user + "/visits"), "");
            System.out.println("visits by user: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 http://" + ip + ":" + app.getPort() + "/locations/" + location + "/avg"), "");
            System.out.println("locations avg: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 http://" + ip + ":" + app.getPort() + "/users/" + user), "");
            System.out.println("user by id: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 http://" + ip + ":" + app.getPort() + "/locations/" + location), "");
            System.out.println("location by id: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 http://" + ip + ":" + app.getPort() + "/visits/" + visit), "");
            System.out.println("visit by id: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            FileTools.text("/tmp/post.lua", "" +
                    "wrk.method = \"POST\"\n" +
                    "wrk.body   = \"" + JsonTools.serialize(app.users.get(user)).replace('"', '\'') + "\"\n" +
                    "wrk.headers[\"Content-Type\"] = \"application/json\"" +
                    "");
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 -s /tmp/post.lua http://" + ip + ":" + app.getPort() + "/users/" + user), "");
            System.out.println("post user update: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            FileTools.text("/tmp/post.lua", "" +
                    "wrk.method = \"POST\"\n" +
                    "wrk.body   = \"" + JsonTools.serialize(app.locations.get(location)).replace('"', '\'') + "\"\n" +
                    "wrk.headers[\"Content-Type\"] = \"application/json\"" +
                    "");
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 -s /tmp/post.lua http://" + ip + ":" + app.getPort() + "/locations/" + location), "");
            System.out.println("post location update: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

            FileTools.text("/tmp/post.lua", "" +
                    "wrk.method = \"POST\"\n" +
                    "wrk.body   = \"" + JsonTools.serialize(app.visits.get(visit)).replace('"', '\'') + "\"\n" +
                    "wrk.headers[\"Content-Type\"] = \"application/json\"" +
                    "");
            exec = Unchecked.ignore(() -> exec("/opt/wrk -c 32 -t 2 -d 1 -s /tmp/post.lua http://" + ip + ":" + app.getPort() + "/visits/" + visit), "");
            System.out.println("post visit update: " + TextTools.find(exec, Pattern.compile("Requests/sec:\\s+\\d+.\\d+")));

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
        System.out.println("\ttotal jsons size: " + sizeCounter.get() / 1024 / 1024 + " MB");
        System.out.println("\tusers: " + users.users.size());
        System.out.println("\tlocations: " + locations.locations.size());
        System.out.println("\tvisits: " + visits.visits.size());

        this.dataLength = sizeCounter.get();
        this.users = initMap(users.users);
        this.locations = initMap(locations.locations);
        this.visits = initMap(visits.visits);

//        this.visitsByUser = new ConcurrentHashMap<>(this.users.size() + 1, 1f);
//        this.visitsByLocation = new ConcurrentHashMap<>(this.locations.size() + 1, 1f);
        this.visitsByUser = HashIntObjMaps.newMutableMap(this.users.size());
        this.visitsByLocation = HashIntObjMaps.newMutableMap(this.locations.size());

        System.out.println("\tMaps created");
        for (Visit visit : visits.visits) {
            addToVisitMaps(visit);
        }
        for (User user : users.users) {
            visitsByUser.computeIfAbsent(user.id, integer -> new ArrayList<>(0)).trimToSize();
        }
        for (Location location : locations.locations) {
            visitsByLocation.computeIfAbsent(location.id, integer -> new ArrayList<>(0)).trimToSize();
        }

        System.out.println("\tvisit maps initialized");

        for (User user : users.users) {
            user.staticResponse = prepareStaticJsonResponse(user);
        }
        System.out.println("\tstatic responses for users: " + users.users.stream().mapToLong(it -> it.staticResponse.length()).sum());
        users.users = null;

        for (Location location : locations.locations) {
            location.staticResponse = prepareStaticJsonResponse(location);
        }
        System.out.println("\tstatic responses for locations: " + locations.locations.stream().mapToLong(it -> it.staticResponse.length()).sum());
        locations.locations = null;

//        for (Visit visit : visits.visits) {
//            visit.staticResponse = prepareStaticJsonResponse(visit);
//        }
//        System.out.println("\tstatic responses for visits: " + visits.visits.stream().mapToLong(it -> it.staticResponse.length()).sum());

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
//                .appendHeader(Header.KV_CONNECTION_KEEP_ALIVE)
//                .appendHeader(Header.KV_CONTENT_TYPE_APPLICATION_JSON)
                .buildStaticResponse();
    }

    private long getSecondsOfAge(int i) {
        return LocalDateTime.now().minus(i, ChronoUnit.YEARS).toEpochSecond(ZoneOffset.UTC); // todo reset current time
    }

    protected <T extends WithId> HashIntObjMap<T> initMap(List<T> data) {
        HashIntObjMap<T> map = HashIntObjMaps.newMutableMap(data.size());
//        Map<Integer, T> map = new ConcurrentHashMap<>(data.size() + 1, 1f);
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

    public static class VisitsReducer implements BinaryOperator<VisitInfo> {

        Holder<byte[]> holder;
        byte[] result;
        int offset;

        protected void init() {
            holder = byteArrayPool.holder();
            result = holder.get();
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
            offset = 11;
        }

        @Override
        public VisitInfo apply(VisitInfo ignored, VisitInfo vi) {
            if (result == null)
                init();

            offset = vi.writeTo(result, offset);
//            System.arraycopy(json, 0, result, offset, json.length);
//            offset += json.length;
            result[offset++] = ',';
            return vi;
        }

        public int finish() {
            result[offset - 1] = ']';
            result[offset] = '}';
            return offset + 1;
        }

        public ReadableByteArray toReadableByteArray() {
            return new ReadableByteArray(result, 0, finish()) {
                @Override
                public void close() {
                    holder.close();
                }
            };
        }
    }
}
