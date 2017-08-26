package ru.highloadcup;

import com.wizzardo.metrics.*;
import com.wizzardo.metrics.system.Utils;
import com.wizzardo.tools.cache.Cache;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.management.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.wizzardo.metrics.system.Utils.*;
import static com.wizzardo.metrics.system.Utils.readInt;

/**
 * Created by Mikhail Bobrutskov on 20.08.17.
 */
public class Monitoring {
    static Cache<Boolean, List<JvmMonitoring.Recordable>> cache;

    public static void initSystemMonitoring() {
        JvmMonitoring jvmMonitoring = new JvmMonitoring(null);
        List<JvmMonitoring.Recordable> recordables = new ArrayList<>();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
//            cache.put(gc.getName(), new GcStats(gc, jvmMonitoring));
            recordables.add(new GcStats(gc, jvmMonitoring));
        }

        for (MemoryPoolMXBean memoryMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
//            cache.put(memoryMXBean.getName(), new MemoryStats(memoryMXBean, jvmMonitoring));
            recordables.add(new MemoryStats(memoryMXBean, jvmMonitoring));
        }

        List<BufferPoolMXBean> bufferPools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (final BufferPoolMXBean bufferPool : bufferPools) {
            JvmMonitoring.Recordable recordable = new JvmMonitoring.Recordable() {

                Recorder.Tags tags = getTags(bufferPool);

                @Override
                public void record(Recorder recorder) {
                    recorder.gauge(jvmMonitoring.getMetricJvmBuffersCount(), bufferPool.getCount(), tags);
                    recorder.gauge(jvmMonitoring.getMetricJvmBuffersMemoryUsed(), bufferPool.getMemoryUsed(), tags);
                    recorder.gauge(jvmMonitoring.getMetricJvmBuffersCapacity(), bufferPool.getTotalCapacity(), tags);
                }

                @Override
                public boolean isValid() {
                    return true;
                }

                protected Recorder.Tags getTags(BufferPoolMXBean memoryPool) {
                    return Recorder.Tags.of("buffer", memoryPool.getName());
                }
            };
//            cache.put("jvm.buffer." + bufferPool.getName(), recordable);
            recordables.add(recordable);
        }
        final CompilationMXBean compilationMXBean = ManagementFactory.getCompilationMXBean();
        JvmMonitoring.Recordable recordable = new JvmMonitoring.Recordable() {
            @Override
            public void record(Recorder recorder) {
                recorder.gauge(jvmMonitoring.getMetricJvmCompilationTime(), compilationMXBean.getTotalCompilationTime());
            }

            @Override
            public boolean isValid() {
                return true;
            }
        };
        recordables.add(new CpuStatReader().createRecordable());
//        cache.put("compilation", recordable);
        recordables.add(recordable);

        com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (threadMXBean.isThreadAllocatedMemorySupported() && threadMXBean.isThreadAllocatedMemoryEnabled()
                && threadMXBean.isThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled()) {
//            cache.put("threading", new ThreadsStats(threadMXBean, jvmMonitoring));
            recordables.add(new ThreadsStats(threadMXBean, jvmMonitoring));
        }

        SimpleClient client = new SimpleClient();
        Recorder recorder = new Recorder(client);
//        cache.onAdd((k, v) -> {
//            for (JvmMonitoring.Recordable r : v) {
//                r.record(recorder);
//            }
//
//            for (Map.Entry<String, List<Event>> entry : client.events.entrySet()) {
//                String name = entry.getKey();
//                for (Event event : entry.getValue()) {
//                    System.out.println(name + ": " + event);
//                }
//            }
//            client.reset();
//        });

        cache = new Cache<>(10);
        cache.onRemove((k, v) -> {
            for (JvmMonitoring.Recordable r : v) {
                r.record(recorder);
            }
            StringBuilder sb = new StringBuilder();
            sb.append("c:").append(App.connectionsCounter.get()).append(";");
            sb.append("u:").append(App.uniqueConnections.size()).append(";");
            sb.append("r:").append(App.requestTimeCounter.getAndSet(0)).append(";");
            sb.append("rc:").append(App.requestCounter.getAndSet(0)).append(";");
            sb.append("ht:").append(App.handleTimeCounter.getAndSet(0)).append(";");
            sb.append("rt:").append(App.readTimeCounter.getAndSet(0)).append(";");
            sb.append("wt:").append(App.writeTimeCounter.getAndSet(0)).append(";");

            sb.append("ugt:").append(App.userGetTimeCounter.getAndSet(0)).append(";");
            sb.append("unt:").append(App.userNewTimeCounter.getAndSet(0)).append(";");
            sb.append("upt:").append(App.userUpdateTimeCounter.getAndSet(0)).append(";");
            sb.append("lgt:").append(App.locationGetTimeCounter.getAndSet(0)).append(";");
            sb.append("lnt:").append(App.locationNewTimeCounter.getAndSet(0)).append(";");
            sb.append("lpt:").append(App.locationUpdateTimeCounter.getAndSet(0)).append(";");
            sb.append("vgt:").append(App.visitGetTimeCounter.getAndSet(0)).append(";");
            sb.append("vnt:").append(App.visitNewTimeCounter.getAndSet(0)).append(";");
            sb.append("vpt:").append(App.visitUpdateTimeCounter.getAndSet(0)).append(";");

            sb.append("vt:").append(App.visitsTimeCounter.getAndSet(0)).append(";");
            sb.append("lt:").append(App.locationsTimeCounter.getAndSet(0)).append(";");

            sb.append("gc:").append(client.events.get("jvm.gc.time").stream().mapToDouble(event -> Double.parseDouble(event.value)).sum()).append(";");
            sb.append("mu:").append(client.events.get("jvm.mp.used").stream().mapToLong(event -> Long.parseLong(event.value) / 1024 / 1024).sum()).append(";");
            sb.append("mc:").append(client.events.get("jvm.mp.committed").stream().mapToLong(event -> Long.parseLong(event.value) / 1024 / 1024).sum()).append(";");
            sb.append("bu:").append(client.events.get("jvm.buffers.memory_used").stream().mapToLong(event -> Long.parseLong(event.value) / 1024 / 1024).sum()).append(";");
//            sb.append("ct:").append(client.events.get("jvm.compilation.time").stream().mapToLong(event -> Long.parseLong(event.value)).sum()).append(";");
            sb.append("a:").append(client.events.getOrDefault("jvm.thread.allocation", Collections.emptyList()).stream()
//                    .filter(event -> event.tags.contains("Thread-0"))
                    .mapToLong(event -> Long.parseLong(event.value) / 1024 / 1024).sum()).append(";");
//            sb.append("c:").append(client.events.getOrDefault("jvm.thread.cpu.nanos", Collections.emptyList()).stream().filter(event -> event.tags.contains("Thread-0")).mapToLong(event -> Long.parseLong(event.value) / 1000 / 1000).sum()).append(";");
//            sb.append("cu:").append(client.events.getOrDefault("jvm.thread.cpu.user.nanos", Collections.emptyList()).stream().filter(event -> event.tags.contains("Thread-0")).mapToLong(event -> Long.parseLong(event.value) / 1000 / 1000).sum()).append(";");
//            sb.append("ct:").append(client.events.getOrDefault("jvm.thread.cpu.nanos", Collections.emptyList()).stream().mapToLong(event -> Long.parseLong(event.value) / 1000 / 1000).sum()).append(";");
//            sb.append("cut:").append(client.events.getOrDefault("jvm.thread.cpu.user.nanos", Collections.emptyList()).stream().mapToLong(event -> Long.parseLong(event.value) / 1000 / 1000).sum()).append(";");
//            sb.append("ctp:").append(client.events.getOrDefault("jvm.thread.cpu.nanos", Collections.emptyList()).stream().mapToLong(event -> Long.parseLong(event.value) / 1000 / 1000).sum() / 100f).append(";");
//
//            sb.append("sid:").append(client.events.getOrDefault("system.cpu.idle", Collections.emptyList()).stream().mapToLong(event -> Long.parseLong(event.value)).sum()).append(";");
//            sb.append("id0:").append(client.events.getOrDefault("system.cpu.core.idle.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu0")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("id1:").append(client.events.getOrDefault("system.cpu.core.idle.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu1")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("id2:").append(client.events.getOrDefault("system.cpu.core.idle.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu2")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("id3:").append(client.events.getOrDefault("system.cpu.core.idle.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu3")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");

//            sb.append("sy0:").append(client.events.getOrDefault("system.cpu.core.system.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu0")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("sy1:").append(client.events.getOrDefault("system.cpu.core.system.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu1")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("sy2:").append(client.events.getOrDefault("system.cpu.core.system.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu2")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("sy3:").append(client.events.getOrDefault("system.cpu.core.system.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu3")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//
//            sb.append("us0:").append(client.events.getOrDefault("system.cpu.core.user.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu0")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("us1:").append(client.events.getOrDefault("system.cpu.core.user.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu1")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("us2:").append(client.events.getOrDefault("system.cpu.core.user.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu2")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("us3:").append(client.events.getOrDefault("system.cpu.core.user.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu3")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");

//            sb.append("st0:").append(client.events.getOrDefault("system.cpu.core.steal.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu0")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("st1:").append(client.events.getOrDefault("system.cpu.core.steal.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu1")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("st2:").append(client.events.getOrDefault("system.cpu.core.steal.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu2")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");
//            sb.append("st3:").append(client.events.getOrDefault("system.cpu.core.steal.p", Collections.emptyList()).stream().filter(event -> event.tags.contains("cpu3")).mapToDouble(event -> round(Double.parseDouble(event.value))).sum()).append(";");

//            for (Map.Entry<String, List<Event>> entry : client.events.entrySet()) {
//                String name = entry.getKey();
//                for (Event event : entry.getValue()) {
//                    System.out.println(name + ": " + event);
//                }
//            }

            System.out.println(sb);
            client.reset();
            cache.put(k, v);
        });

        cache.put(true, recordables);
    }

    public static double round(double d) {
        return Math.round(d * 100) / 100d;
    }

    static class Event {
        String value;
        String tags;

        public Event(String value, String tags) {
            this.value = value;
            this.tags = tags;
        }

        public Event(Object value, String... tags) {
            this.value = String.valueOf(value);
            this.tags = tags != null ? Arrays.toString(tags) : "";
        }

        @Override
        public String toString() {
            return value + " - " + tags;
        }
    }

    private static class SimpleClient implements Client {
        Map<String, List<Event>> events = new LinkedHashMap<>();

        public void reset() {
            events.clear();
        }

        @Override
        public void histogram(String metric, double value, String[] tags) {
            events.computeIfAbsent(metric, s -> new ArrayList<>()).add(new Event(value, tags));
        }

        @Override
        public void histogram(String metric, long value, String[] tags) {
            events.computeIfAbsent(metric, s -> new ArrayList<>()).add(new Event(value, tags));
        }

        @Override
        public void gauge(String metric, long value, String[] tags) {
            events.computeIfAbsent(metric, s -> new ArrayList<>()).add(new Event(value, tags));
        }

        @Override
        public void gauge(String metric, double value, String[] tags) {
            events.computeIfAbsent(metric, s -> new ArrayList<>()).add(new Event(value, tags));
        }

        @Override
        public void increment(String metric, String[] tags) {

        }

        @Override
        public void decrement(String metric, String[] tags) {

        }

        @Override
        public void count(String metric, long value, String[] tags) {
            events.computeIfAbsent(metric, s -> new ArrayList<>()).add(new Event(value, tags));
        }

        @Override
        public void set(String metric, String value, String[] tags) {
            events.computeIfAbsent(metric, s -> new ArrayList<>()).add(new Event(value, tags));
        }
    }

    static public class CpuStatReader {


        public static class CpuStats {
            public String name;
            public long user;
            public long nice;
            public long system;
            public long idle;
            public long iowait;
            public long irq;
            public long softirq;
            public long steal;

            @Override
            public String toString() {
                return "CpuStats{" +
                        "name='" + name + '\'' +
                        ", user=" + user +
                        ", nice=" + nice +
                        ", system=" + system +
                        ", idle=" + idle +
                        ", iowait=" + iowait +
                        ", irq=" + irq +
                        ", softirq=" + softirq +
                        ", steal=" + steal +
                        '}';
            }
        }

        protected int numberOfCores = Runtime.getRuntime().availableProcessors();
        protected byte[] buffer = new byte[10240];
        protected int[] intHolder = new int[1];
        protected long SC_CLK_TCK_MS = 10;

        public CpuStatReader() {
            int userHz = getUserHz();
            if (userHz > 0) {
                SC_CLK_TCK_MS = 1000 / userHz;
            }
        }

        public int getUserHz() {
            try {
                File file = new File("/tmp/CLK_TCK.sh");
                try (FileOutputStream out = new FileOutputStream(file)) {
                    out.write("echo $(getconf CLK_TCK)".getBytes(StandardCharsets.UTF_8));
                }
                String exec = Utils.exec("bash " + file.getAbsolutePath());
                return Integer.parseInt(exec);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return -1;
        }

        public JvmMonitoring.Recordable createRecordable() {
            return new JvmMonitoring.Recordable() {
                Recorder.Tags[] tags;
                CpuStats[] prev;
                CpuStats[] next;
                long time;

                {
                    prev = new CpuStats[Runtime.getRuntime().availableProcessors() + 1];
                    next = new CpuStats[prev.length];
                    tags = new Recorder.Tags[prev.length];
                    for (int i = 1; i < tags.length; i++) {
                        tags[i] = Recorder.Tags.of("cpu", "cpu" + (i - 1));
                    }

                    time = System.nanoTime();
                    read(prev);
                }

                @Override
                public void record(Recorder recorder) {
                    try {
                        read(next);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                    long time = System.nanoTime();
                    long timeMs = (time - this.time) / 1000 / 1000;
                    this.time = time;

                    diff(prev, next);

                    CpuStatReader.this.record(prev[0], recorder, timeMs);
                    for (int i = 1; i < prev.length; i++) {
                        recordWithCore(prev[i], recorder, timeMs, tags[i]);
                    }

                    CpuStats[] temp = prev;
                    prev = next;
                    next = temp;
                }

                @Override
                public boolean isValid() {
                    return true;
                }

            };
        }

        protected void record(CpuStats cpuStats, Recorder recorder, long timeMs) {
            recorder.gauge("system.cpu.user", cpuStats.user * SC_CLK_TCK_MS);
            recorder.gauge("system.cpu.nice", cpuStats.nice * SC_CLK_TCK_MS);
            recorder.gauge("system.cpu.system", cpuStats.system * SC_CLK_TCK_MS);
            recorder.gauge("system.cpu.idle", cpuStats.idle * SC_CLK_TCK_MS);
            recorder.gauge("system.cpu.iowait", cpuStats.iowait * SC_CLK_TCK_MS);
            recorder.gauge("system.cpu.interrupt", (cpuStats.irq + cpuStats.softirq) * SC_CLK_TCK_MS);

            recorder.gauge("system.cpu.user.p", cpuStats.user * 100d * SC_CLK_TCK_MS / timeMs / numberOfCores);
            recorder.gauge("system.cpu.nice.p", cpuStats.nice * 100d * SC_CLK_TCK_MS / timeMs / numberOfCores);
            recorder.gauge("system.cpu.system.p", cpuStats.system * 100d * SC_CLK_TCK_MS / timeMs / numberOfCores);
            recorder.gauge("system.cpu.idle.p", cpuStats.idle * 100d * SC_CLK_TCK_MS / timeMs / numberOfCores);
            recorder.gauge("system.cpu.iowait.p", cpuStats.iowait * 100d * SC_CLK_TCK_MS / timeMs / numberOfCores);
            recorder.gauge("system.cpu.interrupt.p", (cpuStats.irq + cpuStats.softirq) * 100d * SC_CLK_TCK_MS / timeMs / numberOfCores);
        }

        protected void recordWithCore(CpuStats cpuStats, Recorder recorder, long timeMs, Recorder.Tags tags) {
            recorder.gauge("system.cpu.core.user", cpuStats.user * SC_CLK_TCK_MS, tags);
            recorder.gauge("system.cpu.core.nice", cpuStats.nice * SC_CLK_TCK_MS, tags);
            recorder.gauge("system.cpu.core.system", cpuStats.system * SC_CLK_TCK_MS, tags);
            recorder.gauge("system.cpu.core.idle", cpuStats.idle * SC_CLK_TCK_MS, tags);
            recorder.gauge("system.cpu.core.iowait", cpuStats.iowait * SC_CLK_TCK_MS, tags);
            recorder.gauge("system.cpu.core.interrupt", (cpuStats.irq + cpuStats.softirq) * SC_CLK_TCK_MS, tags);

            recorder.gauge("system.cpu.core.user.p", cpuStats.user * 100d * SC_CLK_TCK_MS / timeMs, tags);
            recorder.gauge("system.cpu.core.nice.p", cpuStats.nice * 100d * SC_CLK_TCK_MS / timeMs, tags);
            recorder.gauge("system.cpu.core.system.p", cpuStats.system * 100d * SC_CLK_TCK_MS / timeMs, tags);
            recorder.gauge("system.cpu.core.idle.p", cpuStats.idle * 100d * SC_CLK_TCK_MS / timeMs, tags);
            recorder.gauge("system.cpu.core.iowait.p", cpuStats.iowait * 100d * SC_CLK_TCK_MS / timeMs, tags);
            recorder.gauge("system.cpu.core.steal.p", cpuStats.steal * 100d * SC_CLK_TCK_MS / timeMs, tags);
            recorder.gauge("system.cpu.core.interrupt.p", (cpuStats.irq + cpuStats.softirq) * 100d * SC_CLK_TCK_MS / timeMs, tags);
        }

        protected void diff(CpuStats[] statsA, CpuStats[] statsB) {
            int length = statsA.length;
            for (int i = 0; i < length; i++) {
                CpuStats a = statsA[i];
                CpuStats b = statsB[i];

                a.user = b.user - a.user;
                a.nice = b.nice - a.nice;
                a.system = b.system - a.system;
                a.idle = b.idle - a.idle;
                a.iowait = b.iowait - a.iowait;
                a.irq = b.irq - a.irq;
                a.softirq = b.softirq - a.softirq;
                a.steal = b.steal - a.steal;
            }
        }

        public int read(CpuStats[] stats) {
            byte[] buffer = this.buffer;
            int[] holder = this.intHolder;
            int limit = Utils.read("/proc/stat", buffer);
            if (limit == -1)
                return 0;

            int line = 0;
            int nextLine;
            int position = 0;
            do {
                nextLine = indexOf((byte) '\n', buffer, position, limit);
                if (nextLine == -1)
                    nextLine = limit;

                if (buffer[position] != 'c' || buffer[position + 1] != 'p' || buffer[position + 2] != 'u' || line >= stats.length) // line starts with cpu //|| line > 4
                    break;

                CpuStats cpuStats = stats[line];
                if (cpuStats == null) {
                    stats[line] = (cpuStats = new CpuStats());
                    cpuStats.name = new String(buffer, position, indexOf((byte) ' ', buffer, position, nextLine) - position);
                    position += cpuStats.name.length();
                } else {
                    position = indexOf((byte) ' ', buffer, position, nextLine);
                }
                position = indexOfNot((byte) ' ', buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.user = holder[0];

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.nice = holder[0];

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.system = holder[0];

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.idle = holder[0];

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.iowait = holder[0];

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.irq = holder[0];

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.softirq = holder[0];

                position = readInt(holder, buffer, position, nextLine);
                checkPosition(buffer, position, nextLine);
                cpuStats.steal = holder[0];

                position = nextLine + 1;
                line++;
            } while (position < limit);

            return line;
        }
    }

    protected static void checkPosition(byte[] buffer, int position, int limit) {
        if (position == -1 || position > limit)
            throw new IllegalStateException("Cannot parse: " + new String(buffer, 0, limit));
    }

}
