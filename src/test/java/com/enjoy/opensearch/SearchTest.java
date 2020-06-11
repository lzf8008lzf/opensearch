package com.enjoy.opensearch;

import com.aliyun.opensearch.OpenSearchClient;
import com.aliyun.opensearch.SearcherClient;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Lists;
import com.aliyun.opensearch.sdk.dependencies.org.json.JSONObject;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.search.*;
import com.aliyun.opensearch.sdk.generated.search.general.SearchResult;
import com.aliyun.opensearch.search.SearchParamsBuilder;
import com.aliyun.opensearch.search.SearchResultDebug;
import com.enjoy.opensearch.service.SearchService;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: opensearch
 * @description:
 * @author: LiZhaofu
 * @create: 2020-05-18 09:52
 **/

public class SearchTest {
    private static String appName = SearchService.appName;
    private static String accesskey = SearchService.accesskey;
    private static String secret = SearchService.secret;
    private static String host = SearchService.host;

    public static void main(String[] args) {
        //查看文件和默认编码格式
        System.out.println(String.format("file.encoding: %s", System.getProperty("file.encoding")));
        System.out.println(String.format("defaultCharset: %s", Charset.defaultCharset().name()));
        //创建并构造OpenSearch对象
        OpenSearch openSearch = new OpenSearch(accesskey, secret, host);
        //创建OpenSearchClient对象，并以OpenSearch对象作为构造参数
        OpenSearchClient serviceClient = new OpenSearchClient(openSearch);
        //创建SearcherClient对象，并以OpenSearchClient对象作为构造参数
        SearcherClient searcherClient = new SearcherClient(serviceClient);
        //定义Config对象，用于设定config子句参数，分页或数据返回格式，指定应用名等等
        Config config = new Config(Lists.newArrayList(appName));
        config.setStart(0);
        config.setHits(5);
        //设置返回格式为FULLJSON，目前支持返回 XML，JSON，FULLJSON 等格式
        config.setSearchFormat(SearchFormat.FULLJSON);
        // 设置搜索结果返回应用中哪些字段
        config.setFetchFields(Lists.newArrayList("id", "name", "title"));
        // 注意：config子句中的rerank_size参数，在Rank类对象中设置
        //设置Kvpairs子句参数,此处为distinct子句添加uniq插件，用于reserverd=false时，total及viewtotal不准确问题
        //config.setKvpairs("duniqfield:cate_id");
        // 创建参数对象
        SearchParams searchParams = new SearchParams(config);
        // 设置查询子句，若需多个索引组合查询，需要setQuery处合并，否则若设置多个setQuery后面的会替换前面查询
        searchParams.setQuery("default:'非球面加膜防辐射镜片'");
        // 设置聚合打散子句
        Distinct dist = new Distinct();
        dist.setKey("cate_id"); //设置dist_key
        dist.setDistCount(1); //设置dist_count
        dist.setDistTimes(1); //设置dist_times
        dist.setReserved(false); //设置reserved
        dist.setUpdateTotalHit(false); //设置update_total_hit
        dist.setDistFilter("cate_id<=3"); //设置过滤条件
        dist.setGrade("1.2"); //设置grade
        //此处duniqfield参数通过在config子句中指定kvpairs子句形式添加
        //添加Distinct对象参数
//        searchParams.addToDistincts(dist);
        //设置统计子句
        Aggregate agg = new Aggregate();
        agg.setGroupKey("cate_id"); //设置group_key
        agg.setAggFun("count()"); //设置agg_fun
        agg.setAggFilter("cate_id=1"); //设置agg_filter
        agg.setRange("0~10"); //设置分段统计
        agg.setAggSamplerThresHold("5"); //设置采样阈值
        agg.setAggSamplerStep("5"); //设置采样步长
        agg.setMaxGroup("5"); //设置最大返回组数
        //添加Aggregate对象参数
        //searchParams.addToAggregates(agg);
        // 设置查询过滤条件
        // searchParams.setFilter("id > \"0\""); //此处也可改用后面的ParamsBuilder实现添加过滤条件
        // 设置sort条件
        Sort sorter = new Sort();
        sorter.addToSortFields(new SortField("id", Order.DECREASE)); //设置id字段降序
        sorter.addToSortFields(new SortField("RANK", Order.INCREASE)); //若id相同则以RANK升序
        //添加Sort对象参数
//        searchParams.setSort(sorter);
        // 设置粗精排表达式，此处设置为默认
        Rank rank = new Rank();
        rank.setFirstRankName("default");
        rank.setSecondRankName("default");
        rank.setReRankSize(5);//设置参与精排文档个数
        //添加Aggregate对象参数
//        searchParams.setRank(rank);
        //设置 re_search 重查参数
        //strategy:threshold,params:total_hits#10 => 这里面 strategy:threshold 表示策略，目前只支持一种策略。
        //threshold 的参数是total_hits，表示第一请求的total_hits小于这个值时触发重查。
        Map<String, String> reSearchParams = new HashMap<String, String>();
        reSearchParams.put("re_search", "strategy:threshold,params:total_hits#10");
        //searchParams.setCustomParam(reSearchParams);
        // 设置搜索结果摘要信息，此处采用下面的SearchParamsBuilder对象添加搜索结果摘要，比较简便
        Summary summ = new Summary("name");
        summ.setSummary_field("name");//指定的生效的字段。此字段必需为可分词的text类型的字段。
        summ.setSummary_len("50");//片段长度
        summ.setSummary_element("em"); //飘红标签
        summ.setSummary_ellipsis("...");//片段链接符
        summ.setSummary_snippet("1");//片段数量
        //添加Summary对象参数
        // searchParams.addToSummaries(summ);
        // SearchParams的工具类，提供了更为便捷的操作
        SearchParamsBuilder paramsBuilder = SearchParamsBuilder.create(searchParams);
        // 使用SearchParamsBuilder对象添加搜索结果摘要
//        paramsBuilder.addSummary("name", 50, "em", "...", 1);
        // 设置查询过滤条件
//        paramsBuilder.addFilter("id>=0", "AND");
        try {
            // 执行返回查询结果。用户需按code和message，进行异常情况判断。code对应的错误信息查看——错误码文档。
            SearchResult searchResult = searcherClient.execute(paramsBuilder);
            String result = searchResult.getResult();
            JSONObject obj = new JSONObject(result);
            // 输出查询结果
            System.out.println(obj.toString());
            //个别用户可能需要debug请求地址信息
            SearchResultDebug searchdebugrst = searcherClient.executeDebug(searchParams);
            //输出上次查询请求串信息
            System.out.println(searchdebugrst.getRequestUrl());
        } catch (OpenSearchException e) {
            e.printStackTrace();
        } catch (OpenSearchClientException e) {
            e.printStackTrace();
        }
    }
}
