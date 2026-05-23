package com.example.kafkastub.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Strict JSON → Avro mapping:
 *   - Match by field name.
 *   - Coerce JSON primitives to Avro primitive types where unambiguous.
 *   - On a missing JSON field, fall back to the Avro field's default; if no default
 *     and the field is nullable, write null; otherwise fail.
 *   - Unknown JSON keys (not present in the Avro record) fail loudly.
 */
@Component
public class JsonToAvroMapper {

    public GenericRecord toRecord(JsonNode json, Schema schema) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Root Avro schema must be a record, got " + schema.getType());
        }
        if (json == null || !json.isObject()) {
            throw new IllegalArgumentException("Root JSON must be an object for record " + schema.getFullName());
        }
        return (GenericRecord) buildRecord(json, schema, schema.getFullName());
    }

    private Object buildRecord(JsonNode json, Schema schema, String path) {
        GenericData.Record record = new GenericData.Record(schema);

        var avroFieldNames = schema.getFields().stream()
                .map(Schema.Field::name)
                .collect(Collectors.toSet());

        Iterator<String> it = json.fieldNames();
        while (it.hasNext()) {
            String key = it.next();
            if (!avroFieldNames.contains(key)) {
                throw new IllegalArgumentException(
                        "Unknown JSON field '" + key + "' at " + path + " — not declared in Avro schema " + schema.getFullName());
            }
        }

        for (Schema.Field field : schema.getFields()) {
            String fieldPath = path + "." + field.name();
            JsonNode value = json.get(field.name());

            if (value == null || value.isNull()) {
                if (field.hasDefaultValue()) {
                    record.put(field.name(), GenericData.get().getDefaultValue(field));
                } else if (isNullable(field.schema())) {
                    record.put(field.name(), null);
                } else {
                    throw new IllegalArgumentException(
                            "Missing required field '" + field.name() + "' at " + fieldPath + " (no default, not nullable)");
                }
            } else {
                record.put(field.name(), convert(value, field.schema(), fieldPath));
            }
        }
        return record;
    }

    private Object convert(JsonNode json, Schema schema, String path) {
        return switch (schema.getType()) {
            case RECORD -> buildRecord(json, schema, path);
            case STRING -> json.isTextual() ? json.asText() : json.toString();
            case INT -> coerceInt(json, path);
            case LONG -> coerceLong(json, path);
            case FLOAT -> (float) coerceDouble(json, path);
            case DOUBLE -> coerceDouble(json, path);
            case BOOLEAN -> coerceBoolean(json, path);
            case BYTES -> ByteBuffer.wrap(bytesOf(json));
            case FIXED -> new GenericData.Fixed(schema, bytesOf(json));
            case NULL -> null;
            case ENUM -> new GenericData.EnumSymbol(schema, json.asText());
            case ARRAY -> buildArray(json, schema, path);
            case MAP -> buildMap(json, schema, path);
            case UNION -> resolveUnion(json, schema, path);
        };
    }

    private GenericData.Array<Object> buildArray(JsonNode json, Schema schema, String path) {
        if (!json.isArray()) {
            throw new IllegalArgumentException("Expected JSON array at " + path + ", got " + json.getNodeType());
        }
        List<Object> items = new ArrayList<>(json.size());
        int i = 0;
        for (JsonNode el : json) {
            items.add(convert(el, schema.getElementType(), path + "[" + i + "]"));
            i++;
        }
        return new GenericData.Array<>(schema, items);
    }

    private Map<String, Object> buildMap(JsonNode json, Schema schema, String path) {
        if (!json.isObject()) {
            throw new IllegalArgumentException("Expected JSON object at " + path + ", got " + json.getNodeType());
        }
        Map<String, Object> m = new HashMap<>();
        json.fields().forEachRemaining(e ->
                m.put(e.getKey(), convert(e.getValue(), schema.getValueType(), path + "." + e.getKey())));
        return m;
    }

    private Object resolveUnion(JsonNode json, Schema schema, String path) {
        // Pick the first non-null branch that matches the JSON shape.
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) continue;
            if (jsonMatchesBranch(json, branch)) {
                return convert(json, branch, path);
            }
        }
        throw new IllegalArgumentException("No union branch in " + schema + " matches JSON at " + path + ": " + json);
    }

    private static boolean jsonMatchesBranch(JsonNode json, Schema branch) {
        return switch (branch.getType()) {
            case STRING, ENUM -> json.isTextual();
            case INT, LONG -> json.isIntegralNumber() || (json.isTextual() && isIntegral(json.asText()));
            case FLOAT, DOUBLE -> json.isNumber() || (json.isTextual() && isNumeric(json.asText()));
            case BOOLEAN -> json.isBoolean() || (json.isTextual() && ("true".equalsIgnoreCase(json.asText()) || "false".equalsIgnoreCase(json.asText())));
            case ARRAY -> json.isArray();
            case MAP, RECORD -> json.isObject();
            case BYTES, FIXED -> json.isTextual() || json.isBinary();
            case NULL -> json.isNull();
            case UNION -> false;
        };
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return false;
        return schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
    }

    private static int coerceInt(JsonNode json, String path) {
        if (json.isIntegralNumber()) return json.intValue();
        if (json.isTextual()) {
            try { return Integer.parseInt(json.asText().trim()); }
            catch (NumberFormatException e) { throw typeError(json, "int", path); }
        }
        throw typeError(json, "int", path);
    }

    private static long coerceLong(JsonNode json, String path) {
        if (json.isIntegralNumber()) return json.longValue();
        if (json.isTextual()) {
            try { return Long.parseLong(json.asText().trim()); }
            catch (NumberFormatException e) { throw typeError(json, "long", path); }
        }
        throw typeError(json, "long", path);
    }

    private static double coerceDouble(JsonNode json, String path) {
        if (json.isNumber()) return json.doubleValue();
        if (json.isTextual()) {
            try { return Double.parseDouble(json.asText().trim()); }
            catch (NumberFormatException e) { throw typeError(json, "double", path); }
        }
        throw typeError(json, "double", path);
    }

    private static boolean coerceBoolean(JsonNode json, String path) {
        if (json.isBoolean()) return json.booleanValue();
        if (json.isTextual()) {
            String s = json.asText().trim();
            if ("true".equalsIgnoreCase(s)) return true;
            if ("false".equalsIgnoreCase(s)) return false;
        }
        throw typeError(json, "boolean", path);
    }

    private static byte[] bytesOf(JsonNode json) {
        try {
            if (json.isBinary()) return json.binaryValue();
        } catch (Exception ignored) { /* fall through */ }
        return json.asText().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static boolean isIntegral(String s) {
        try { Long.parseLong(s.trim()); return true; } catch (NumberFormatException e) { return false; }
    }

    private static boolean isNumeric(String s) {
        try { Double.parseDouble(s.trim()); return true; } catch (NumberFormatException e) { return false; }
    }

    private static IllegalArgumentException typeError(JsonNode json, String avroType, String path) {
        return new IllegalArgumentException("Cannot convert JSON " + json.getNodeType() + " value " + json + " to Avro " + avroType + " at " + path);
    }
}
