package ru.highloadcup;

import com.wizzardo.tools.misc.NumberToChars;
import com.wizzardo.tools.misc.UTF8;
import com.wizzardo.tools.reflection.StringReflection;
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

//    protected int mark;
//    protected long visitedAt;
//    protected String place;
//    protected byte[] json;

    public VisitInfo(Visit visit, Location location, User user) {
        this.visit = visit;
        this.location = location;
        this.user = user;
//        json = prepareJson();
    }

//    private byte[] prepareJson() {
//        return ("{\"mark\":" + (mark = visit.mark) + ",\"visited_at\":" + (visitedAt = visit.visited_at) + ",\"place\":\"" + (place = location.place) + "\"}")
//                .getBytes(StandardCharsets.UTF_8);
//    }
//
//    public byte[] getJson() {
//        if (json != null && mark == visit.mark && visitedAt == visit.visited_at && place == location.place)
//            return json;
//        else
//            return json = prepareJson();
//    }

    @Override
    public String toString() {
        return "VisitInfo{" +
                "visit=" + visit +
                ", location=" + location +
                ", user=" + user +
                '}';
    }

    public int writeTo(byte[] bytes, int offset) {
        bytes[offset++] = '{';

        bytes[offset++] = '"';
        bytes[offset++] = 'm';
        bytes[offset++] = 'a';
        bytes[offset++] = 'r';
        bytes[offset++] = 'k';
        bytes[offset++] = '"';
        bytes[offset++] = ':';
        offset = NumberToChars.toChars(visit.mark, bytes, offset);

        bytes[offset++] = ',';
        bytes[offset++] = '"';
        bytes[offset++] = 'v';
        bytes[offset++] = 'i';
        bytes[offset++] = 's';
        bytes[offset++] = 'i';
        bytes[offset++] = 't';
        bytes[offset++] = 'e';
        bytes[offset++] = 'd';
        bytes[offset++] = '_';
        bytes[offset++] = 'a';
        bytes[offset++] = 't';
        bytes[offset++] = '"';
        bytes[offset++] = ':';
        offset = NumberToChars.toChars(visit.visited_at, bytes, offset);

        bytes[offset++] = ',';
        bytes[offset++] = '"';
        bytes[offset++] = 'p';
        bytes[offset++] = 'l';
        bytes[offset++] = 'a';
        bytes[offset++] = 'c';
        bytes[offset++] = 'e';
        bytes[offset++] = '"';
        bytes[offset++] = ':';
        bytes[offset++] = '"';
        offset = encode(StringReflection.chars(location.place), 0, location.place.length(), bytes, offset);
        bytes[offset++] = '"';
        bytes[offset++] = '}';

        return offset;
    }

    public static int encode(char[] chars, int off, int length, byte[] bytes, int offset) {
        int limit = off + length;

        int l;
        char ch;
        for (l = offset; off < limit; bytes[l++] = (byte) ch) {
            if ((ch = chars[off++]) >= 128) {
                --off;
                break;
            }
        }

        while (true) {
            while (off < limit) {
                char c = chars[off++];
                if (c < 128) {
                    bytes[l++] = (byte) c;
                } else if (c < 2048) {
                    bytes[l++] = (byte) (192 | c >> 6);
                    bytes[l++] = (byte) (128 | c & 63);
                } else if (c >= '\ud800' && c < '\ue000') {
                    throw new IllegalArgumentException("Not implemented");
//                    int r = off < limit?parseSurrogate(c, chars[off]):-1;
//                    if(r < 0) {
//                        bytes[l++] = 63;
//                    } else {
//                        bytes[l++] = (byte)(240 | r >> 18);
//                        bytes[l++] = (byte)(128 | r >> 12 & 63);
//                        bytes[l++] = (byte)(128 | r >> 6 & 63);
//                        bytes[l++] = (byte)(128 | r & 63);
//                        ++off;
//                    }
                } else {
                    bytes[l++] = (byte) (224 | c >> 12);
                    bytes[l++] = (byte) (128 | c >> 6 & 63);
                    bytes[l++] = (byte) (128 | c & 63);
                }
            }

            return l;
        }
    }
}
