package com.example.kafkastub.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Mode mode,
        Integer recordsPerTopic,
        @NotEmpty List<TopicSpec> topics
) {
    public AppProperties {
        if (mode == null) mode = Mode.ONESHOT;
        if (recordsPerTopic == null || recordsPerTopic < 1) recordsPerTopic = 1;
    }

    public enum Mode { ONESHOT, SERVER }

    public record TopicSpec(
            @NotBlank String key,
            @NotBlank String topic,
            @NotBlank String jsonPath,
            @NotBlank String avroSchemaPath,
            Integer recordCount,
            String messageKeyField
    ) {}
}
