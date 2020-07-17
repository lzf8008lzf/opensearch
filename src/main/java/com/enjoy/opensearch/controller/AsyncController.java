package com.enjoy.opensearch.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Callable;

/**
 * @program: opensearch
 * @description:
 * @author: LiZhaofu
 * @create: 2020-07-17 17:25
 **/

@RestController
@Slf4j
public class AsyncController {

    @RequestMapping("/async")
    public Callable<String> async() throws InterruptedException {
        log.info("主线程开始");

        Callable<String> result = new Callable<String>() {

            @Override
            public String call() throws Exception {
                log.info("副线程开始");
                Thread.sleep(1000);
                log.info("副线程结束");
                return "success";
            }

        };
        log.info("主线程返回");
        return result;

    }
}