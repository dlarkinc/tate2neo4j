package demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class Tate2mongoApplication {

    public static void main(String[] args) {
        SpringApplication.run(Tate2mongoApplication.class, args);
    }
}
