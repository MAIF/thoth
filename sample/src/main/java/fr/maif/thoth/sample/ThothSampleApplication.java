package fr.maif.thoth.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jooq.JooqAutoConfiguration;

@SpringBootApplication(exclude = JooqAutoConfiguration.class)
public class ThothSampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(ThothSampleApplication.class, args);
	}

}
