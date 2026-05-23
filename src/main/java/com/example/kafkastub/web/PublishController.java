package com.example.kafkastub.web;

import com.example.kafkastub.config.AppProperties;
import com.example.kafkastub.mapping.JsonToAvroMapper;
import com.example.kafkastub.mapping.SchemaLoader;
import com.example.kafkastub.producer.AvroPublisher;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/publish")
@ConditionalOnProperty(name = "app.mode", havingValue = "SERVER")
public class PublishController {

    private final AppProperties props;
    private final SchemaLoader schemaLoader;
    private final JsonToAvroMapper mapper;
    private final AvroPublisher publisher;

    public PublishController(AppProperties props,
                             SchemaLoader schemaLoader,
                             JsonToAvroMapper mapper,
                             AvroPublisher publisher) {
        this.props = props;
        this.schemaLoader = schemaLoader;
        this.mapper = mapper;
        this.publisher = publisher;
    }

    /** Publish using the sample JSON file configured for {key}. Repeats `count` times (default 1). */
    @PostMapping("/{key}")
    public ResponseEntity<Map<String, Object>> publishConfigured(
            @PathVariable String key,
            @RequestParam(defaultValue = "1") int count) {
        AppProperties.TopicSpec spec = findSpec(key);
        Schema schema = schemaLoader.loadSchema(spec.avroSchemaPath());
        JsonNode json = schemaLoader.loadJson(spec.jsonPath());

        int sent = sendAll(spec, schema, json, count);
        return ResponseEntity.ok(Map.of("topic", spec.topic(), "sent", sent));
    }

    /** Publish using an inline JSON body against the configured schema for {key}. */
    @PostMapping(value = "/{key}/inline", consumes = "application/json")
    public ResponseEntity<Map<String, Object>> publishInline(
            @PathVariable String key,
            @RequestParam(defaultValue = "1") int count,
            @RequestBody JsonNode body) {
        AppProperties.TopicSpec spec = findSpec(key);
        Schema schema = schemaLoader.loadSchema(spec.avroSchemaPath());
        int sent = sendAll(spec, schema, body, count);
        return ResponseEntity.ok(Map.of("topic", spec.topic(), "sent", sent));
    }

    private int sendAll(AppProperties.TopicSpec spec, Schema schema, JsonNode json, int count) {
        int sent = 0;
        if (json.isArray()) {
            for (JsonNode el : json) sent += sendOne(spec, schema, el, count);
        } else {
            sent += sendOne(spec, schema, json, count);
        }
        return sent;
    }

    private int sendOne(AppProperties.TopicSpec spec, Schema schema, JsonNode element, int count) {
        GenericRecord record = mapper.toRecord(element, schema);
        String messageKey = (spec.messageKeyField() != null && !spec.messageKeyField().isBlank()
                && element.get(spec.messageKeyField()) != null)
                ? element.get(spec.messageKeyField()).asText()
                : null;
        for (int i = 0; i < count; i++) {
            publisher.publish(spec.topic(), record, messageKey);
        }
        return count;
    }

    private AppProperties.TopicSpec findSpec(String key) {
        return props.topics().stream()
                .filter(t -> t.key().equals(key))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No topic configured with key='" + key + "'"));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
