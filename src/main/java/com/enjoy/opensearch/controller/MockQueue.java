package com.enjoy.opensearch.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @program: opensearch
 * @description:
 * @author: LiZhaofu
 * @create: 2020-07-17 17:36
 **/

@Component
@Slf4j
public class MockQueue {

    // 生成下单
    private String placeOrder;

    // 完成下单
    private String completeOrder;

    public String getPlaceOrder() {
        return placeOrder;
    }

    public void setPlaceOrder(String placeOrder) {
        new Thread(() -> {
            log.info("接到下单请求");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.completeOrder = placeOrder;
            log.info("下单请求处理完毕," + placeOrder);
        }).start();

    }

    public String getCompleteOrder() {
        return completeOrder;
    }

    public void setCompleteOrder(String completeOrder) {
        this.completeOrder = completeOrder;
    }
}
