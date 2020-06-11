package com.enjoy.opensearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @program: opensearch
 * @description:
 * @author: LiZhaofu
 * @create: 2020-05-16 13:10
 **/

@SpringBootApplication
@EnableAutoConfiguration
public class StartApplication {
    public static void main(String[] args) {
        SpringApplication.run(StartApplication.class, args);
    }
}
