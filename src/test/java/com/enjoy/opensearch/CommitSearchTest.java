package com.enjoy.opensearch;

/**
 * @program: opensearch
 * @description:
 * @author: LiZhaofu
 * @create: 2020-05-18 09:54
 **/

import com.aliyun.opensearch.DocumentClient;
import com.aliyun.opensearch.OpenSearchClient;
import com.aliyun.opensearch.SearcherClient;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Lists;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Maps;
import com.aliyun.opensearch.sdk.dependencies.org.json.JSONObject;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchResult;
import com.aliyun.opensearch.sdk.generated.search.Config;
import com.aliyun.opensearch.sdk.generated.search.SearchFormat;
import com.aliyun.opensearch.sdk.generated.search.SearchParams;
import com.aliyun.opensearch.sdk.generated.search.general.SearchResult;
import com.enjoy.opensearch.service.SearchService;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;
public class CommitSearchTest {
    private static String appName = SearchService.appName;
    private static String accesskey = SearchService.accesskey;
    private static String secret = SearchService.secret;
    private static String host = SearchService.host;
    private static String tableName = "替换opensearch应用表名";

    public static void main(String[] args) {
        //查看文件和默认编码格式
        System.out.println(String.format("file.encoding: %s", System.getProperty("file.encoding")));
        System.out.println(String.format("defaultCharset: %s", Charset.defaultCharset().name()));
        //生成随机数，作为主键值
        Random rand = new Random();
        int value = rand.nextInt(Integer.MAX_VALUE);
        //定义Map对象存储上传数据doc1
        Map<String, Object> doc1 = Maps.newLinkedHashMap();
        doc1.put("id", value);
        String title_string = "Commit方式新增文档1";// utf-8
        byte[] bytes;
        try {
            bytes = title_string.getBytes("utf-8");
            String utf8_string = new String(bytes, "utf-8");
            doc1.put("name", utf8_string);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        doc1.put("phone", "13712345678");
        int[] int_arr = {33,44};
        doc1.put("int_arr", int_arr);
        String[] literal_arr = {"Commit方式新增文档1","测试Commit方式新增文档1]"};
        doc1.put("literal_arr", literal_arr);
        float[] float_arr = {(float)1.1,(float)1.2};
        doc1.put("float_arr", float_arr);
        doc1.put("cate_id", 1);
        //创建并构造OpenSearch对象
        OpenSearch openSearch1 = new OpenSearch(accesskey, secret, host);
        //创建OpenSearchClient对象，并以OpenSearch对象作为构造参数
        OpenSearchClient serviceClient1 = new OpenSearchClient(openSearch1);
        //定义DocumentClient对象添加数据并提交
        DocumentClient documentClient1 = new DocumentClient(serviceClient1);
        // 把doc1加入缓存，并设为新增文档
        documentClient1.add(doc1);
        //文档输出
        System.out.println(doc1.toString());
        try {
            //执行提交新增操作，此处用于测试故单个提交延迟10s查看操作信息，也可在后面一次性提交执行操作
            OpenSearchResult osr = documentClient1.commit(appName, tableName);
            //判断数据是否推送成功，主要通过判断2处，第一处判断用户方推送是否成功，第二处是应用控制台中有无报错日志
            //用户方推送成功后，也有可能在应用端执行失败，此错误会直接在应用控制台错误日志中生成，比如字段内容转换失败
            if(osr.getResult().equalsIgnoreCase("true")){
                System.out.println("用户方推送无报错！\n以下为getTraceInfo推送请求Id:"+osr.getTraceInfo().getRequestId());
            }else{
                System.out.println("用户方推送报错！"+osr.getTraceInfo());
            }
        } catch (OpenSearchException e) {
            e.printStackTrace();
        } catch (OpenSearchClientException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(10000);//休眠10秒，可在控制台查看新增的数据
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //定义Map对象doc2，并更新doc1，更新也采用add，已存在数据且主键值相同即为更新处理
        Map<String, Object> doc2 = Maps.newLinkedHashMap();
        doc2.put("id", value);
        String title_string2 = "Commit方式更新文档1";// utf-8
        byte[] bytes2;
        try {
            bytes2 = title_string2.getBytes("utf-8");
            String utf8_string2 = new String(bytes2, "utf-8");
            doc2.put("name", utf8_string2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        doc2.put("phone", "13712342222");
        int[] int_arr2 = {22,22};
        doc2.put("int_arr", int_arr2);
        String[] literal_arr2 = {"Commit方式更新文档1","Commit方式更新文档1"};
        doc2.put("literal_arr", literal_arr2);
        float[] float_arr2 = {(float)1.1,(float)1.2};
        doc2.put("float_arr", float_arr2);
        doc2.put("cate_id", 1);
        // 把doc2加入缓存，因为此数据已存在，且主键值也相同，此记录作更新处理
        documentClient1.add(doc2);
        //文档输出
        System.out.println(doc2.toString());
        try {
            // 执行更新并提交，此处用于测试故单个提交延迟10s查看操作信息，也可在后面一次性提交执行操作
            OpenSearchResult osr = documentClient1.commit(appName, tableName);
            //判断数据是否推送成功，主要通过判断2处，第一处判断用户方推送是否成功，第二处是应用控制台中有无报错日志
            //用户方推送成功后，也有可能在应用端执行失败，此错误会直接在应用控制台错误日志中生成，比如字段内容转换失败
            if(osr.getResult().equalsIgnoreCase("true")){
                System.out.println("用户方推送无报错！\n以下为getTraceInfo推送请求Id:"+osr.getTraceInfo().getRequestId());
            }else{
                System.out.println("用户方推送报错！"+osr.getTraceInfo());
            }
        } catch (OpenSearchException e) {
            e.printStackTrace();
        } catch (OpenSearchClientException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(10000);//休眠10秒，可在控制台查看更新的数据
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 定义Map对象doc3，删除文档只需指定需删除文档主键值即可
        Map<String, Object> doc3 = Maps.newLinkedHashMap();
        doc3.put("id", value);
        // 把doc3加入缓存，此处做删除文档处理
        documentClient1.remove(doc3);
        //文档输出
        System.out.println(doc3.toString());
        try {
            // 执行删除并提交，此处用于测试故单个提交延迟10s查看操作信息，也可在后面一次性提交执行操作
            OpenSearchResult osr = documentClient1.commit(appName, tableName);
            //判断数据是否推送成功，主要通过判断2处，第一处判断用户方推送是否成功，第二处是应用控制台中有无报错日志
            //用户方推送成功后，也有可能在应用端执行失败，此错误会直接在应用控制台错误日志中生成，比如字段内容转换失败
            if(osr.getResult().equalsIgnoreCase("true")){
                System.out.println("用户方推送无报错！\n以下为getTraceInfo推送请求Id:"+osr.getTraceInfo().getRequestId());
            }else{
                System.out.println("用户方推送报错！"+osr.getTraceInfo());
            }
        } catch (OpenSearchException e) {
            e.printStackTrace();
        } catch (OpenSearchClientException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(10000);//休眠10秒后，再查看删除后的数据，如果此处不休眠而是立刻查询可能会因为数据没有及时删除而查出存在的数据，至少休眠1秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //-------------查询删除后的文档，此处因为查询删除后的问题还是存在，暂不提供查询代码，怀疑有延迟-----------------
        //创建并构造OpenSearch对象
        OpenSearch openSearch2 = new OpenSearch(accesskey, secret, host);
        //创建OpenSearchClient对象，并以OpenSearch对象作为构造参数
        OpenSearchClient serviceClient2 = new OpenSearchClient(openSearch2);
        //创建SearcherClient对象，并以OpenSearchClient对象作为构造参数
        SearcherClient searcherClient2 = new SearcherClient(serviceClient2);
        //创建Config对象，用于设定config子句参数，分页或数据返回格式等等
        Config config = new Config(Lists.newArrayList(appName));
        config.setStart(0);
        config.setHits(30);
        //设置返回格式为json,目前只支持返回xml和json格式，暂不支持返回fulljson类型
        config.setSearchFormat(SearchFormat.JSON);
        SearchParams searchParams = new SearchParams(config);
        searchParams.setQuery("id:'" + value + "'");
        //// 执行返回查询结果
        SearchResult searchResult;
        try {
            searchResult = searcherClient2.execute(searchParams);
            String result = searchResult.getResult();
            JSONObject obj = new JSONObject(result);
            // 输出查询结果
            System.out.println("查询调试输出:"+obj.toString());
        } catch (OpenSearchException e) {
            e.printStackTrace();
        } catch (OpenSearchClientException e) {
            e.printStackTrace();
        }
    }
}
