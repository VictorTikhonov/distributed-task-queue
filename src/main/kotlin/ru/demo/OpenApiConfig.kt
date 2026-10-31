package ru.demo

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig {

    @Bean
    fun customOpenAPI(): OpenAPI {
        return OpenAPI()
            .info(
                Info()
                    .title("Distributed Task Queue API")
                    .version("1.0.0")
                    .description(
                        "REST API распределённой очереди задач на основе алгоритма Raft"
                    )
            )
    }
}