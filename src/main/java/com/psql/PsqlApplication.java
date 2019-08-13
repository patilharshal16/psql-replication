package com.psql;

import com.psql.service.PsqlParserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import static java.lang.System.exit;

@SpringBootApplication
@EnableConfigurationProperties({AppConfig.class})
public class PsqlApplication implements CommandLineRunner {

    @Autowired
    PsqlParserService psqlParserService;

    public static void main(String[] args) {
        SpringApplication.run(PsqlApplication.class, args);
    }

    @Bean
    public AppConfig appConfig() {
        return new AppConfig();
    }


    @Override
    public void run(String... args) throws Exception {
        psqlParserService.parseData();
        exit(0);
    }
}
