package com.ufgov.xxzx.task;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.horizon.client.validate.HttpToken;

public class SingTask {

    private synchronized static void exeQuery() {

        System.out.println("开始执行查询:时间" + System.currentTimeMillis());
        Connection conn = null;
        try {
            Class.forName(CTURLConstants.ORACLEDRIVER);
            conn = DriverManager.getConnection(CTURLConstants.DBURL, CTURLConstants.DBUSER, CTURLConstants.PASSWORD);// 连接数据库
            conn.setAutoCommit(false);
            Map<String, List<JSONObject>> map = query(conn);
            if (map.size() <= 0) {
                System.out.println("没有要更新的数据:" + System.currentTimeMillis());
                conn.close();
                return;
            }
            String token = HttpToken.getToken();
            if (token == null) {
                token = HttpToken.getToken();
            }
            JSONObject json = JSONObject.parseObject(token);
            String code = (String) json.get("code");
            String data = "";
            if (StringUtils.isNotBlank(code)) {
                if (code.equals("1")) {
                    data = json.getString("data");
                } else {
                    data = json.getString("msg");
                }
            }
            Statement statement = conn.createStatement();
            for (String key : map.keySet()) {
                CloseableHttpResponse response = buildPost(map.get(key), data);
                System.out.print("原始响应：" + response.getEntity());
                String res = EntityUtils.toString(response.getEntity(), "UTF-8");
                System.out.print("解析之后的响应：" + res);
                JSONObject resjson = JSON.parseObject(res);
                int returncode = resjson.getIntValue("code");
                if (returncode == 1) {
                    String sql = "update ct_mid set status='1' where ctno='" + map.get(key).get(0).get("ctno") + "' and billno='" + map.get(key).get(0).get("billno") + "' ";
                    System.out.println("合同更新SQL:" + sql);
                    statement.executeUpdate(sql);
                } else {
                    String errmsg = resjson.getString("msg");
                    System.out.println("调用合同回写出错:" + errmsg);
                    throw new Exception("调用合同回写出错:" + errmsg);
                }
            }
            conn.commit();
        } catch (Exception e) {
            System.out.print(e.getMessage());
            if(conn!=null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    System.out.print(e1.getMessage());
                }
            }
        } finally {
            if(conn!=null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.out.print(e.getMessage());
                }
            }
        }

    }
    private static CloseableHttpResponse buildPost(List<JSONObject> temp, String data) throws Exception {
        JSONObject params = new JSONObject();
        params.put("ctno", temp.get(0).get("ctno"));
        params.put("billno", temp.get(0).get("billno"));
        params.put("applytime", temp.get(0).get("applytime"));
        params.put("paytime", temp.get(0).get("paytime"));
        params.put("append", temp.get(0).get("append"));
        params.put("userid", temp.get(0).get("userid"));
        params.put("qkNote", temp.get(0).get("qkNote"));
        JSONArray arrs = new JSONArray();
        for (JSONObject obj : temp) {
            JSONObject pro = new JSONObject();
            pro.put("procode", obj.get("procode"));
            pro.put("paymount", obj.get("paymount"));
            arrs.add(pro);
        }
        params.put("proinfo", arrs);
        System.out.println("执行接口调用:" + params.toJSONString());
        LinkedHashMap<String, Object> par = new LinkedHashMap<String, Object>();
        par.put(CTURLConstants.APPCODE, CTURLConstants.APPCODE_VALUE);
        par.put(CTURLConstants.SIGN, data);
        par.put(CTURLConstants.PARAMS, params.toJSONString());
        CloseableHttpResponse response = (CloseableHttpResponse) post(par, CTURLConstants.BAISICALURL
                + CTURLConstants.WIRTEBACKPRJ);
        return response;
    }

    private static Map<String, List<JSONObject>> query(Connection conn) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from ct_mid where status='0' for update ");
//		ResultSet rs = statement.executeQuery("select * from ct_mid where status='0' ");
        Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
        while (rs != null && rs.next()) {
            String ctno = String.valueOf(rs.getString("CTNO"));
            String billno = String.valueOf(rs.getString("BILLNO"));
            String applytime = String.valueOf(rs.getString("APPLYTIME"));
            String paytime = String.valueOf(rs.getString("PAYTIME"));
            String append = String.valueOf(rs.getString("APPEND"));
            String procode = String.valueOf(rs.getString("PROCODE"));
            String paymount = String.valueOf(rs.getString("PAYMOUNT"));
            String userid = String.valueOf(rs.getString("ca_serial"));
            String qkNote = String.valueOf(rs.getString("reason"));
            String key = ctno + billno + applytime + append;
            JSONObject jsonobj = new JSONObject();
            jsonobj.put("ctno", ctno);
            jsonobj.put("billno", billno);
            jsonobj.put("applytime", applytime);
            jsonobj.put("paytime", paytime);
            jsonobj.put("append", append);
            jsonobj.put("procode", procode);
            jsonobj.put("paymount", paymount);
            jsonobj.put("userid", userid);
            jsonobj.put("qkNote", qkNote);
            List<JSONObject> temp = null;
            if (map.containsKey(key)) {
                temp = map.get(key);
            } else {
                temp = new ArrayList<JSONObject>();
            }
            temp.add(jsonobj);
            map.put(key, temp);
        }
        return map;
    }


    private static HttpResponse post(Map<String, Object> param, String url) throws Exception {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(url);

        // StringEntity entity = new StringEntity(params, "UTF-8");

        List<NameValuePair> paramList = new ArrayList<NameValuePair>();
        for (String key : param.keySet()) {
            paramList.add(new BasicNameValuePair(key, (String) param.get(key)));
        } //
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(paramList, "UTF-8");
        httpPost.setEntity(entity);
        try {
            System.out.println(entity.toString());
            CloseableHttpResponse response = httpClient.execute(httpPost);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }


    public static void main(String[] args) {
        try {
            ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
            service.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        //付款信息回写
                        exeQuery();
                        //合同删除和审批信息调用
                        exeDelPass();
                    } catch (Exception e) {
                        System.out.print(e.getMessage());
                    }
                }
            }, 3000, 60000, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static synchronized void exeDelPass() {

        System.out.println("开始执行查询:时间" + System.currentTimeMillis());
        Connection conn = null;
        try {
            Class.forName(CTURLConstants.ORACLEDRIVER);
            conn = DriverManager.getConnection(CTURLConstants.DBURL, CTURLConstants.DBUSER, CTURLConstants.PASSWORD);
            conn.setAutoCommit(false);
            Map<String, List<JSONObject>> map = queryCtDelAndPass(conn);
            if (map.size() <= 0) {
                System.out.println("没有要更新的数据:" + System.currentTimeMillis());
                conn.close();
                return;
            }
            String token = HttpToken.getToken();
            if (token == null) {
                token = HttpToken.getToken();
            }
            JSONObject json = JSONObject.parseObject(token);
            String code = (String) json.get("code");
            String data = "";
            if (StringUtils.isNotBlank(code)) {
                if (code.equals("1")) {
                    data = json.getString("data");
                } else {
                    data = json.getString("msg");
                }
            }
            Statement statement = conn.createStatement();
            for (String key : map.keySet()) {
                CloseableHttpResponse response = buildDelPassPost(map.get(key), data);
                System.out.print("原始响应：" + response.getEntity());
                String res = EntityUtils.toString(response.getEntity(), "UTF-8");
                System.out.print("解析之后的响应：" + res);
                JSONObject resjson = JSON.parseObject(res);
                int returncode = resjson.getIntValue("code");
                if (returncode == 200) {
                    String sql = "update pd_xmtb set pd_zt='1' where row_id='" +map.get(key).get(0).getString("rowId")
                            + "' and pd_czbs='" + map.get(key).get(0).get("czbs") + "' ";
                    System.out.println("合同更新SQL:" + sql);
                    statement.executeUpdate(sql);
                } else {
                    String errmsg = resjson.getString("msg");
                    System.out.println("调用合同回写出错:" + errmsg);
                    throw new Exception("调用合同回写出错:" + errmsg);
                }
            }
            conn.commit();
        } catch (Exception e) {
            System.out.print(e.getMessage());
            if(conn!=null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    System.out.print(e1.getMessage());
                }
            }
        } finally {
            if(conn!=null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.out.print(e.getMessage());
                }
            }
        }

    }

    private static Map<String, List<JSONObject>> queryCtDelAndPass(Connection conn) throws SQLException{
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from pd_xmtb where pd_zt='0' for update ");
        Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
        while (rs != null && rs.next()) {
            String rowId = String.valueOf(rs.getString("row_id"));
            String pdXh = String.valueOf(rs.getString("pd_xh"));
            String czbs = String.valueOf(rs.getString("pd_czbs"));
            String nd = String.valueOf(rs.getString("nd"));
            JSONObject jsonobj = new JSONObject();
            jsonobj.put("rowId", rowId);
            jsonobj.put("pdXh", pdXh);
            jsonobj.put("czbs", czbs);
            jsonobj.put("nd",nd);
            String key = rowId+czbs;
            List<JSONObject> temp = null;
            if (map.containsKey(key)) {
                temp = map.get(key);
            } else {
                temp = new ArrayList<JSONObject>();
            }
            temp.add(jsonobj);
            map.put(key, temp);
        }
        return map;
    }

    private static CloseableHttpResponse buildDelPassPost(List<JSONObject> temp, String data) throws Exception {
        JSONObject params = new JSONObject();
        params.put("procode", temp.get(0).get("pdXh"));
        params.put("status", temp.get(0).get("czbs"));
        params.put("budgetyear", temp.get(0).get("nd"));
        System.out.println("执行接口调用:" + params.toJSONString());
        LinkedHashMap<String, Object> par = new LinkedHashMap<String, Object>();
        par.put(CTURLConstants.APPCODE, CTURLConstants.APPCODE_VALUE);
        par.put(CTURLConstants.SIGN, data);
        par.put(CTURLConstants.PARAMS, params.toJSONString());
        CloseableHttpResponse response = (CloseableHttpResponse) post(par, CTURLConstants.BAISICALURL
                + CTURLConstants.PUSHPRO);
        return response;
    }

}
