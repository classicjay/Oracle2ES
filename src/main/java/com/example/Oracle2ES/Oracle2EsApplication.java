package com.example.Oracle2ES;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.net.UnknownHostException;

@SpringBootApplication
@ComponentScan
@EnableAutoConfiguration
@Configuration
@CrossOrigin(origins = "*")
public class Oracle2EsApplication {

	public static void main(String[] args) throws UnknownHostException {
		SpringApplication.run(Oracle2EsApplication.class, args);
	}
}
