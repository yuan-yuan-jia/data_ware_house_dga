package com.atguigu.dga.governance.utils;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class HttpUtil {


    static OkHttpClient okHttpClient = new OkHttpClient();


    public static String get(String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        Response response = okHttpClient
                .newCall(request)
                .execute();


        try(ResponseBody body = response.body()) {
            return body.string();
        }

    }

    public static void main(String[] args) throws IOException {
        String s = HttpUtil.get("http://hadoop102:18080/api/v1/applications/application_1684083580862_0012");
        System.out.println(s);
    }


}
