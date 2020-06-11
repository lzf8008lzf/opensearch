package com.enjoy.opensearch.service;

import lombok.extern.slf4j.Slf4j;
import com.aliyun.opensearch.DocumentClient;
import com.aliyun.opensearch.OpenSearchClient;
import com.aliyun.opensearch.SearcherClient;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Lists;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchResult;
import com.aliyun.opensearch.sdk.generated.search.Config;
import com.aliyun.opensearch.sdk.generated.search.Order;
import com.aliyun.opensearch.sdk.generated.search.SearchFormat;
import com.aliyun.opensearch.sdk.generated.search.SearchParams;
import com.aliyun.opensearch.sdk.generated.search.Sort;
import com.aliyun.opensearch.sdk.generated.search.SortField;
import com.aliyun.opensearch.sdk.generated.search.general.SearchResult;
/**
 * @program: opensearch
 * @description: 阿里云搜索服务
 * @author: LiZhaofu
 * @create: 2020-05-16 14:29
 **/

@Slf4j
public class SearchService {

    public static String appName = "enjoy_opensearch";//应用名称
    public static String accesskey = "";//您的阿里云的Access Key ID
    public static String secret = "";//"阿里云 Access Key ID 对应的 Access Key Secret";
    public static String host = "http://opensearch-cn-qingdao.aliyuncs.com";//"这里的host需要根据访问应用基本信息页中提供的的API入口来确定";

    //创建并构造OpenSearch对象
    public static OpenSearch openSearch = new OpenSearch(accesskey, secret, host);
    //创建OpenSearchClient对象，并以OpenSearch对象作为构造参数
    public static OpenSearchClient serviceClient = new OpenSearchClient(openSearch);

    public void uploadBatchData(String data) throws OpenSearchException, OpenSearchClientException{
        DocumentClient doc = new DocumentClient(serviceClient);
        String tableName = "main";//"要上传数据的表名";
        OpenSearchResult osr = doc.push(data, appName, tableName);
        log.info("uploadBatchData result:{}",osr.getResult());
    }

    public String search(String param,int start,int hists) throws OpenSearchException, OpenSearchClientException{
        String result ="";

        //创建SearcherClient对象，并以OpenSearchClient对象作为构造参数
        SearcherClient searcherClient = new SearcherClient(serviceClient);
        //定义Config对象，用于设定config子句参数，指定应用名，分页，数据返回格式等等
        Config config = new Config(Lists.newArrayList(appName));
        config.setStart(start);
        config.setHits(hists);
        //设置返回格式为fulljson格式
        config.setSearchFormat(SearchFormat.JSON);
        // 创建参数对象
        SearchParams searchParams = new SearchParams(config);
        // 指定搜索的关键词，这里要指定在哪个索引上搜索，如果不指定的话默认在使用“default”索引（索引字段名称是您在您的数据结构中的“索引字段列表”中对应字段。），若需多个索引组合查询，需要在setQuery处合并，否则若设置多个setQuery子句，则后面的子句会替换前面子句
        //searchParams.setQuery("name:'搜索'");
        //searchParams.setQuery("default:'"+param+"'");
        searchParams.setQuery(param);
        //设置查询过滤条件
        //searchParams.setFilter("id>0");
        //创建sort对象，并设置二维排序
        Sort sort = new Sort();
        //设置id字段降序
        sort.addToSortFields(new SortField("id", Order.DECREASE));
        //若id相同则以RANK相关性算分升序
        sort.addToSortFields(new SortField("RANK", Order.INCREASE));
        //添加Sort对象参数
        //searchParams.setSort(sort);
        //执行查询语句返回数据对象
        SearchResult searchResult = searcherClient.execute(searchParams);
        //以字符串返回查询数据
        result = searchResult.getResult();

        return result;
    }
}
