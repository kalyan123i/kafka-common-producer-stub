package com.example.kafkastub.runner;

import com.example.kafkastub.config.AppProperties;
import com.example.kafkastub.mapping.JsonPlaceholderResolver;
import com.example.kafkastub.mapping.JsonToAvroMapper;
import com.example.kafkastub.mapping.SchemaLoader;
import com.example.kafkastub.producer.AvroPublisher;
import tools.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConditionalOnProperty(name = "app.mode", havingValue = "ONESHOT", matchIfMissing = true)
public class OneShotRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(OneShotRunner.class);

    private final AppProperties props;
    private final SchemaLoader schemaLoader;
    private final JsonPlaceholderResolver placeholderResolver;
    private final JsonToAvroMapper mapper;
    private final AvroPublisher publisher;
    private final ConfigurableApplicationContext context;

    public OneShotRunner(AppProperties props,
                         SchemaLoader schemaLoader,
                         JsonPlaceholderResolver placeholderResolver,
                         JsonToAvroMapper mapper,
                         AvroPublisher publisher,
                         ConfigurableApplicationContext context) {
        this.props = props;
        this.schemaLoader = schemaLoader;
        this.placeholderResolver = placeholderResolver;
        this.mapper = mapper;
        this.publisher = publisher;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        int exitCode = 0;
        try {
            for (AppProperties.TopicSpec spec : props.topics()) {
                publishTopic(spec);
            }
        } catch (RuntimeException e) {
            log.error("One-shot publishing failed: {}", e.getMessage(), e);
            exitCode = 1;
        } finally {
            int code = exitCode;
            new Thread(() -> System.exit(SpringApplication.exit(context, () -> code)), "oneshot-exit").start();
        }
    }

    private void publishTopic(AppProperties.TopicSpec spec) {
        Schema schema = schemaLoader.loadSchema(spec.avroSchemaPath());
        JsonNode json = schemaLoader.loadJson(spec.jsonPath());
        int perElement = spec.recordCount() != null && spec.recordCount() > 0
                ? spec.recordCount()
                : props.recordsPerTopic();

        List<JsonNode> elements = new ArrayList<>();
        if (json.isArray()) {
            json.forEach(elements::add);
        } else {
            elements.add(json);
        }

        log.info("Topic '{}': {} JSON record(s) x {} repetition(s) → {} message(s)",
                spec.topic(), elements.size(), perElement, elements.size() * perElement);

        for (JsonNode template : elements) {
            for (int i = 0; i < perElement; i++) {
                JsonNode resolved = placeholderResolver.resolve(template);
                GenericRecord record = mapper.toRecord(resolved, schema);
                String key = resolveKey(resolved, spec);
                publisher.publish(spec.topic(), record, key);
            }
        }
    }

    private static String resolveKey(JsonNode element, AppProperties.TopicSpec spec) {
        if (spec.messageKeyField() == null || spec.messageKeyField().isBlank()) return null;
        JsonNode k = element.get(spec.messageKeyField());
        return k == null || k.isNull() ? null : k.asString();
    }
}
