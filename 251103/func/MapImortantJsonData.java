package com.stream.realtime.lululemon.func;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONArray;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.net.InetAddress;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;

import java.io.File;
import java.io.IOException;

public class MapImortantJsonData extends RichMapFunction<JSONObject, JSONObject> {

    // 用于时间格式化的SimpleDateFormat（线程安全方式）
    private transient ThreadLocal<SimpleDateFormat> dateFormat;

    // GeoIP reader（transient，不可序列化）
    private transient DatabaseReader dbReader;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化日期格式化器
        dateFormat = ThreadLocal.withInitial(() -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC")); // 根据需求设置时区
            return sdf;
        });

        // 初始化 GeoIP 数据库：优先从环境变量 GEOIP_DB 指定路径加载
        try {
            String geoDbPath = System.getenv("GEOIP_DB");
            if (geoDbPath == null || geoDbPath.isEmpty()) {
                // 如果没有设置环境变量，你可以在这里放一个默认路径（可修改）
                geoDbPath = "D:\\dev-env\\GeoLite2-City.mmdb\\GeoLite2-City.mmdb";
            }
            File database = new File(geoDbPath);
            if (database.exists() && database.isFile()) {
                dbReader = new DatabaseReader.Builder(database).build();
            } else {
                // 如果找不到 DB 文件，记录警告（或使用空实现）
                dbReader = null;
                System.err.println("GeoIP DB file not found at: " + geoDbPath + "  — IP to region lookup will be disabled.");
            }
        } catch (Exception e) {
            dbReader = null;
            System.err.println("Failed to init GeoIP DatabaseReader: " + e.getMessage());
        }
    }

    @Override
    public JSONObject map(JSONObject data) throws Exception {

        if (data.containsKey("device") && data.getJSONObject("device") != null) {
            String log_id = data.getString("log_id");
            JSONObject device = data.getJSONObject("device");

            // 注意：原代码是 data.getJSONObject("gis").getString("ip");
            // 为防 NPE，加检查
            String ip = null;
            if (data.containsKey("gis") && data.getJSONObject("gis") != null) {
                ip = data.getJSONObject("gis").getString("ip");
            }

            String net = null;
            if (data.containsKey("network") && data.getJSONObject("network") != null) {
                net = data.getJSONObject("network").getString("net");
            }

            String opa = data.getString("opa");
            String log_type = data.getString("log_type");
            String ts = data.getString("ts");
            String product_id = data.getString("product_id");
            String order_id = data.getString("order_id");
            String user_id = data.getString("user_id");
            String keywords = data.getString("keywords");

            // 1. 将keywords从JSON数组字符串转换为逗号分隔的字符串
            if (keywords != null && !keywords.isEmpty()) {
                try {
                    JSONArray keywordArray = JSONArray.parseArray(keywords);
                    StringBuilder keywordBuilder = new StringBuilder();
                    for (int i = 0; i < keywordArray.size(); i++) {
                        if (i > 0) {
                            keywordBuilder.append(",");
                        }
                        keywordBuilder.append(keywordArray.getString(i));
                    }
                    keywords = keywordBuilder.toString();
                } catch (Exception e) {
                    // 如果解析失败，保持原样或设置为空字符串
                    keywords = "";
                }
            }

            // 2. 将时间戳转换为格式化时间
            String formattedTime = "Invalid Time";
            if (ts != null && !ts.isEmpty()) {
                try {
                    double timestampDouble = Double.parseDouble(ts);
                    long timestampMs;
                    // 如果数值大于等于1e12（即1970年以后的毫秒级时间戳），则认为是毫秒级
                    if (timestampDouble >= 1e12) {
                        // 毫秒级，取整
                        timestampMs = (long) timestampDouble;
                    } else {
                        // 秒级，乘以1000转换为毫秒
                        timestampMs = (long) (timestampDouble * 1000);
                    }
                    formattedTime = dateFormat.get().format(new Date(timestampMs));
                } catch (Exception e) {
                    formattedTime = "时间格式错误";
                }
            }

            // 新增：IP -> 地区解析
            String country = "";
            String province = "";
            String city = "";
            String region = ""; // 拼接字段，例如 "中国 广西 南宁"

            if (ip != null && !ip.isEmpty() && dbReader != null) {
                try {
                    InetAddress ipAddress = InetAddress.getByName(ip);
                    CityResponse response = dbReader.city(ipAddress);

                    // 优先使用中文名称（"zh-CN"），回退到默认 getName()
                    if (response.getCountry() != null) {
                        if (response.getCountry().getNames() != null && response.getCountry().getNames().get("zh-CN") != null) {
                            country = response.getCountry().getNames().get("zh-CN");
                        } else {
                            country = response.getCountry().getName() == null ? "" : response.getCountry().getName();
                        }
                    }
                    if (response.getMostSpecificSubdivision() != null) {
                        if (response.getMostSpecificSubdivision().getNames() != null && response.getMostSpecificSubdivision().getNames().get("zh-CN") != null) {
                            province = response.getMostSpecificSubdivision().getNames().get("zh-CN");
                        } else {
                            province = response.getMostSpecificSubdivision().getName() == null ? "" : response.getMostSpecificSubdivision().getName();
                        }
                    }
                    if (response.getCity() != null) {
                        if (response.getCity().getNames() != null && response.getCity().getNames().get("zh-CN") != null) {
                            city = response.getCity().getNames().get("zh-CN");
                        } else {
                            city = response.getCity().getName() == null ? "" : response.getCity().getName();
                        }
                    }

                } catch (Exception e) {
                    // 解析失败则留空或用 "未知"
                    // System.err.println("GeoIP lookup fail for ip=" + ip + " : " + e.getMessage());
                }
            } else {
                // 没有 db 或 ip，region 为空
            }

            // 拼接 region 字段，去除空串
            StringBuilder regionBuilder = new StringBuilder();
            if (country != null && !country.isEmpty()) regionBuilder.append(country);
            if (province != null && !province.isEmpty()) {
                if (regionBuilder.length() > 0) regionBuilder.append(" ");
                regionBuilder.append(province);
            }
            if (city != null && !city.isEmpty()) {
                if (regionBuilder.length() > 0) regionBuilder.append(" ");
                regionBuilder.append(city);
            }
            region = regionBuilder.toString();

            // 将字段写入 device
            device.put("log_id", log_id);
            device.put("ip", ip);
            device.put("net", net);
            device.put("opa", opa);
            device.put("log_type", log_type);
            device.put("ts", ts); // 保留原始时间戳
            device.put("formatted_time", formattedTime); // 新增格式化时间字段
            device.put("product_id", product_id);
            device.put("order_id", order_id);
            device.put("user_id", user_id);
            device.put("keywords", keywords);

            // 新增地区相关字段
            device.put("country", country);
            device.put("province", province);
            device.put("city", city);
            device.put("region", region);

            return device;
        }

        return null;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (dbReader != null) {
            try {
                dbReader.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
