package com.example.kafkastub.mapping;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Replaces ${placeholder} tokens inside JSON string values on each invocation.
 * Supported built-ins:
 *   ${uuid}        → fresh random UUID
 *   ${now.millis}  → System.currentTimeMillis()
 *   ${now.iso}     → Instant.now() in ISO-8601
 * Unknown placeholders pass through unchanged.
 *
 * The resolver deep-copies the template before mutating, so the loaded JSON template
 * stays intact and every call yields fresh values.
 */
@Component
public class JsonPlaceholderResolver {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    public JsonNode resolve(JsonNode template) {
        JsonNode copy = template.deepCopy();
        walk(copy);
        return copy;
    }

    private void walk(JsonNode node) {
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            List<String> names = new ArrayList<>(obj.propertyNames());
            for (String name : names) {
                JsonNode v = obj.get(name);
                if (v.isString()) {
                    String resolved = resolveString(v.asString());
                    if (!resolved.equals(v.asString())) {
                        obj.set(name, JsonNodeFactory.instance.stringNode(resolved));
                    }
                } else if ((v.isObject() || v.isArray())) {
                    walk(v);
                }
            }
        } else if (node.isArray()) {
            ArrayNode arr = (ArrayNode) node;
            for (int i = 0; i < arr.size(); i++) {
                JsonNode el = arr.get(i);
                if (el.isString()) {
                    String resolved = resolveString(el.asString());
                    if (!resolved.equals(el.asString())) {
                        arr.set(i, JsonNodeFactory.instance.stringNode(resolved));
                    }
                } else if ((el.isObject() || el.isArray())) {
                    walk(el);
                }
            }
        }
    }

    private static String resolveString(String s) {
        if (s.indexOf("${") < 0) return s;
        Matcher m = PLACEHOLDER.matcher(s);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1).trim();
            String value = switch (name) {
                case "uuid" -> UUID.randomUUID().toString();
                case "now.millis" -> Long.toString(System.currentTimeMillis());
                case "now.iso" -> Instant.now().toString();
                default -> m.group(0); // leave unknown placeholders alone
            };
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }
}
