package com.example.kafkastub.mapping;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import org.apache.avro.Schema;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

@Component
public class SchemaLoader {

    // Construct directly instead of injecting: Spring Boot's auto-configured ObjectMapper is the
    // Jackson 2 one. JsonMapper instances are immutable and safe to share.
    private static final JsonMapper JSON_MAPPER = JsonMapper.builder().build();

    private final ResourceLoader resourceLoader;

    public SchemaLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    public Schema loadSchema(String path) {
        Resource r = resourceLoader.getResource(normalize(path));
        try (InputStream in = r.getInputStream()) {
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load Avro schema from '" + path + "': " + e.getMessage(), e);
        }
    }

    public JsonNode loadJson(String path) {
        Resource r = resourceLoader.getResource(normalize(path));
        try (InputStream in = r.getInputStream()) {
            return JSON_MAPPER.readTree(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load JSON from '" + path + "': " + e.getMessage(), e);
        }
    }

    private static String normalize(String path) {
        // accept "classpath:foo", "file:/tmp/foo", or a bare path treated as a filesystem path
        if (path.contains(":")) return path;
        return "file:" + path;
    }
}
