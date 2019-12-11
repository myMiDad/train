package com.tom.datasource;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import javax.swing.text.html.parser.Entity;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ClassName: WeatherJson
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/10 14:03
 */
public class WeatherJson {
    public static String getWeatherInfo(int longitude, int latitude){
        StringBuffer sb = new StringBuffer("https://free-api.heweather.net/s6/weather/now?location=");
        sb.append(longitude).append(",").append(latitude).append("&key=77b0a658dbce455ba56f71c923de603e");

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet(sb.toString());

        String weaAndHum = "";

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            String line = EntityUtils.toString(entity, StandardCharsets.UTF_8);
            weaAndHum = parseJson(line);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try{
                if (httpClient!=null){
                    httpClient.close();
                }
                if (response!=null){
                    response.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
        return weaAndHum;
    }

    public static String parseJson(String line){
        String wea ="";
        String hum = "";
        JSONObject jsonObject = JSON.parseObject(line);

        JSONArray heWeather6 = jsonObject.getJSONArray("HeWeather6");
        JSONObject jsonArray = heWeather6.getJSONObject(0);
//            {"now":{"hum":"23","vis":"1","pres":"1013","pcpn":"0.0","fl":"4","wind_sc":"3","wind_dir":"北风","wind_spd":"16","cloud":"0","wind_deg":"6","tmp":"9","cond_txt":"晴","cond_code":"100"},"update":{"loc":"2019-12-10 15:36","utc":"2019-12-10 07:36"},"basic":{"admin_area":"北京","tz":"+8.00","location":"东城","lon":"116.41875458","parent_city":"北京","cnty":"中国","lat":"39.91754532","cid":"CN101011600"},"status":"ok"}
        JSONObject now = jsonArray.getJSONObject("now");
//            {"hum":"23","vis":"1","pres":"1012","pcpn":"0.0","fl":"4","wind_sc":"3","wind_dir":"西风","wind_spd":"18","cloud":"0","wind_deg":"260","tmp":"9","cond_txt":"多云","cond_code":"101"}

        String cond_txt = now.getString("cond_txt");
        String wind_dir = now.getString("wind_dir");
        hum = now.getString("hum");

        if(StringUtils.isEmpty(cond_txt)){
            cond_txt="";
        }
        if (StringUtils.isEmpty(wind_dir)){
            wind_dir="";
        }
        if (cond_txt.contains("雨")){
            wea = "雨";
        }else if(cond_txt.contains("雪")){
            wea="雪";
        }else if (cond_txt.contains("冰雹")){
            wea = "冰雹";
        }else if (cond_txt.contains("浮沉")){
            wea = "浮沉";
        }else if (cond_txt.contains("扬沙")){
            wea = "扬沙";
        }else {
            if (wind_dir.contains("风")){
                wea = "风";
            }else {
                wea = "晴";
            }
        }

        return wea+"|"+hum;
    }
    public static void main(String[] args) {
        String weatherInfo = getWeatherInfo(114, 60);
        System.out.println(weatherInfo);
    }
}
