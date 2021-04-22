package com.geminit;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;

public class Test {
    public static void main(String[] args) {
        String s = String.valueOf(null);
        String url = "http://192.168.2.211:11015/v3/namespaces/default/apps/ttt";
        String res = httpClient("DELETE", url, "{\"name\": \"tyx\", \"age\": 23}");
        System.out.println(res);
    }

    public static String httpClient(String postType, String url, String jsonStr) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse httpResponse = null;
        String result = null;
        try {
            if (postType.equals("GET")) {
                //===== get =====//
                HttpGet httpGet = new HttpGet(url);
                //设置header
                httpGet.setHeader("Content-type", "application/json");
                httpGet.setHeader("DataEncoding", "UTF-8");
                httpGet.setHeader("Authorization",
                        "Bearer  Aghyb290AOS2lYnEXOSmyNvEXNbZnrcFQHcl/TK2P5vJci6ICM8GaeBI0BIJoanHQGS7MW9ZRupS");
                //发送请求
                httpResponse = httpClient.execute(httpGet);
            } else if(postType.equals("POST")) {
                //===== post =====//
                HttpPost httpPost = new HttpPost(url);
                httpPost.setHeader("Content-type", "application/json");
                httpPost.setHeader("DataEncoding", "UTF-8");
                httpPost.setHeader("Authorization",
                        "Bearer  Aghyb290AOS2lYnEXOSmyNvEXNbZnrcFQHcl/TK2P5vJci6ICM8GaeBI0BIJoanHQGS7MW9ZRupS");
                //增加参数
                httpPost.setEntity(new StringEntity(jsonStr.toString(),"UTF-8"));
                httpResponse = httpClient.execute(httpPost);
            } else if(postType.equals("PUT")) {
                //===== put =====//
                HttpPut httpPut = new HttpPut(url);
                httpPut.setHeader("Content-type", "application/json");
                httpPut.setHeader("DataEncoding", "UTF-8");
                httpPut.setHeader("Authorization",
                        "Bearer  Aghyb290AOS2lYnEXOSmyNvEXNbZnrcFQHcl/TK2P5vJci6ICM8GaeBI0BIJoanHQGS7MW9ZRupS");
                httpResponse = httpClient.execute(httpPut);
            } else if (postType.equals("DELETE")) {
                //===== delete =====//
                HttpDelete httpDelete = new HttpDelete(url);
                httpDelete.setHeader("Content-type", "application/json");
                httpDelete.setHeader("DataEncoding", "UTF-8");
                httpDelete.setHeader("Authorization",
                        "Bearer  Aghyb290AOS2lYnEXOSmyNvEXNbZnrcFQHcl/TK2P5vJci6ICM8GaeBI0BIJoanHQGS7MW9ZRupS");
                httpResponse = httpClient.execute(httpDelete);
            }
            //返回结果
            HttpEntity entity = httpResponse.getEntity();
            result = EntityUtils.toString(entity);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
