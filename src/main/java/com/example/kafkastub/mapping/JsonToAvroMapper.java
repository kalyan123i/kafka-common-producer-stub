package com.example.kafkastub.mapping;

import tools.jackson.databind.JsonNode;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
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

    /** Sentinel: convertLogical didn't recognize the logical type, fall back to primitive handling. */
    private static final Object UNHANDLED = new Object();

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

        for (String key : json.propertyNames()) {
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
        LogicalType logical = schema.getLogicalType();
        if (logical != null) {
            Object v = convertLogical(json, schema, logical, path);
            if (v != UNHANDLED) return v;
        }
        return switch (schema.getType()) {
            case RECORD -> buildRecord(json, schema, path);
            case STRING -> json.isString() ? json.asString() : json.toString();
            case INT -> coerceInt(json, path);
            case LONG -> coerceLong(json, path);
            case FLOAT -> (float) coerceDouble(json, path);
            case DOUBLE -> coerceDouble(json, path);
            case BOOLEAN -> coerceBoolean(json, path);
            case BYTES -> ByteBuffer.wrap(bytesOf(json));
            case FIXED -> new GenericData.Fixed(schema, bytesOf(json));
            case NULL -> null;
            case ENUM -> new GenericData.EnumSymbol(schema, json.asString());
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
        json.properties().forEach(e ->
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
        LogicalType lt = branch.getLogicalType();
        if (lt != null) {
            switch (lt.getName()) {
                case "date", "time-millis", "time-micros",
                     "timestamp-millis", "timestamp-micros",
                     "local-timestamp-millis", "local-timestamp-micros":
                    return json.isString() || json.isIntegralNumber();
                case "uuid":
                    return json.isString();
                default:
                    // fall through to underlying primitive check
            }
        }
        return switch (branch.getType()) {
            case STRING, ENUM -> json.isString();
            case INT, LONG -> json.isIntegralNumber() || (json.isString() && isIntegral(json.asString()));
            case FLOAT, DOUBLE -> json.isNumber() || (json.isString() && isNumeric(json.asString()));
            case BOOLEAN -> json.isBoolean() || (json.isString() && ("true".equalsIgnoreCase(json.asString()) || "false".equalsIgnoreCase(json.asString())));
            case ARRAY -> json.isArray();
            case MAP, RECORD -> json.isObject();
            case BYTES, FIXED -> json.isString() || json.isBinary();
            case NULL -> json.isNull();
            case UNION -> false;
        };
    }

    /**
     * Handle common Avro logical types. The value stored in the GenericRecord is the underlying
     * primitive (int days for date, long millis for timestamp-millis, etc.) — KafkaAvroSerializer
     * encodes it and the schema's logicalType tag is preserved on the wire.
     */
    private Object convertLogical(JsonNode json, Schema schema, LogicalType logical, String path) {
        String name = logical.getName();
        try {
            return switch (name) {
                case "date" -> {
                    if (json.isIntegralNumber()) yield json.intValue();
                    String s = json.asString();
                    yield (int) parseLocalDateLenient(s).toEpochDay();
                }
                case "time-millis" -> {
                    if (json.isIntegralNumber()) yield json.intValue();
                    yield (int) (LocalTime.parse(json.asString()).toNanoOfDay() / 1_000_000L);
                }
                case "time-micros" -> {
                    if (json.isIntegralNumber()) yield json.longValue();
                    yield LocalTime.parse(json.asString()).toNanoOfDay() / 1_000L;
                }
                case "timestamp-millis" -> {
                    if (json.isIntegralNumber()) yield json.longValue();
                    yield parseInstantLenient(json.asString()).toEpochMilli();
                }
                case "timestamp-micros" -> {
                    if (json.isIntegralNumber()) yield json.longValue();
                    Instant i = parseInstantLenient(json.asString());
                    yield Math.multiplyExact(i.getEpochSecond(), 1_000_000L) + i.getNano() / 1_000L;
                }
                case "local-timestamp-millis" -> {
                    if (json.isIntegralNumber()) yield json.longValue();
                    yield LocalDateTime.parse(json.asString()).toInstant(ZoneOffset.UTC).toEpochMilli();
                }
                case "local-timestamp-micros" -> {
                    if (json.isIntegralNumber()) yield json.longValue();
                    LocalDateTime ldt = LocalDateTime.parse(json.asString());
                    Instant i = ldt.toInstant(ZoneOffset.UTC);
                    yield Math.multiplyExact(i.getEpochSecond(), 1_000_000L) + i.getNano() / 1_000L;
                }
                case "uuid" -> json.asString();
                default -> UNHANDLED; // decimal etc. — fall back to primitive handling
            };
        } catch (DateTimeParseException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cannot convert JSON " + json + " to Avro logical type '" + name + "' at " + path + ": " + e.getMessage(), e);
        }
    }

    /** Accept "YYYY-MM-DD" as well as a full ISO timestamp (in which case take the UTC date portion). */
    private static LocalDate parseLocalDateLenient(String s) {
        try {
            return LocalDate.parse(s);
        } catch (DateTimeParseException primary) {
            try {
                return OffsetDateTime.parse(s).atZoneSameInstant(ZoneOffset.UTC).toLocalDate();
            } catch (DateTimeParseException ignored) {
                throw primary;
            }
        }
    }

    /** Accept ISO instant ("2026-01-01T00:00:00Z") or offset-date-time; otherwise let Instant.parse decide. */
    private static Instant parseInstantLenient(String s) {
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException primary) {
            try {
                return OffsetDateTime.parse(s).toInstant();
            } catch (DateTimeParseException ignored) {
                throw primary;
            }
        }
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return false;
        return schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
    }

    private static int coerceInt(JsonNode json, String path) {
        if (json.isIntegralNumber()) return json.intValue();
        if (json.isString()) {
            try { return Integer.parseInt(json.asString().trim()); }
            catch (NumberFormatException e) { throw typeError(json, "int", path); }
        }
        throw typeError(json, "int", path);
    }

    private static long coerceLong(JsonNode json, String path) {
        if (json.isIntegralNumber()) return json.longValue();
        if (json.isString()) {
            try { return Long.parseLong(json.asString().trim()); }
            catch (NumberFormatException e) { throw typeError(json, "long", path); }
        }
        throw typeError(json, "long", path);
    }

    private static double coerceDouble(JsonNode json, String path) {
        if (json.isNumber()) return json.doubleValue();
        if (json.isString()) {
            try { return Double.parseDouble(json.asString().trim()); }
            catch (NumberFormatException e) { throw typeError(json, "double", path); }
        }
        throw typeError(json, "double", path);
    }

    private static boolean coerceBoolean(JsonNode json, String path) {
        if (json.isBoolean()) return json.booleanValue();
        if (json.isString()) {
            String s = json.asString().trim();
            if ("true".equalsIgnoreCase(s)) return true;
            if ("false".equalsIgnoreCase(s)) return false;
        }
        throw typeError(json, "boolean", path);
    }

    private static byte[] bytesOf(JsonNode json) {
        try {
            if (json.isBinary()) return json.binaryValue();
        } catch (Exception ignored) { /* fall through */ }
        return json.asString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
