package com.example.kafkastub.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

@Component
public class SchemaLoader {

    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;

    public SchemaLoader(ResourceLoader resourceLoader, ObjectMapper objectMapper) {
        this.resourceLoader = resourceLoader;
        this.objectMapper = objectMapper;
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
            return objectMapper.readTree(in);
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
