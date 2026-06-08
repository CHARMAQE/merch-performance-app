package com.smollan.backend.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI merchPerformanceOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Merch Performance Backend API")
                        .version("v1.0")
                        .description("Spring Boot REST APIs for mobile supervision, store monitoring, "
                                + "validation issue review, and operational dashboard access in the "
                                + "merchandising performance solution."))
                .addServersItem(new Server().url("http://localhost:9000"));
    }
}
