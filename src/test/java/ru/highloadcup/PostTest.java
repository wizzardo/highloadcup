package ru.highloadcup;

import com.wizzardo.tools.http.Request;
import com.wizzardo.tools.http.Response;
import com.wizzardo.tools.json.JsonObject;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Mikhail Bobrutskov on 11.08.17.
 */
public class PostTest {

    @Test
    public void simplePost() throws IOException {
        Response response = new Request("http://localhost:8080/users/1")
                .header("Connection", "Close")
                .json(new JsonObject()
                        .append("email", "wibylcudestiwuk@icloud.com")
//                        .append("email", "12312")
                        .toString()
                ).post();

        System.out.println("status: " + response.getResponseCode());
        System.out.println("headers: " + response.headers());
        System.out.println("body: " + response.asString());
    }
}
