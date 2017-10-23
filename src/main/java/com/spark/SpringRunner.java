package com.spark;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

import javax.annotation.Resource;

@SpringBootApplication(exclude = {
    GsonAutoConfiguration.class,
    DataSourceAutoConfiguration.class,
    DataSourceTransactionManagerAutoConfiguration.class,
    HibernateJpaAutoConfiguration.class})
public class SpringRunner implements CommandLineRunner {

  @Resource
  Config config;

  @Override
  public void run(String... args) throws Exception {

    System.out.println(config.getAppName());

  }

  public static void main(String[] args) {

    try {
      SpringApplication.run(SpringRunner.class, args);
    } catch (Throwable ex) {
      ex.printStackTrace();
      throw ex;
    }

  }
}
