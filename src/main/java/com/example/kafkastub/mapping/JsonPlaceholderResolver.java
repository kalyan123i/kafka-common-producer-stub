package com.example.kafkastub.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
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
            List<String> names = new ArrayList<>();
            obj.fieldNames().forEachRemaining(names::add);
            for (String name : names) {
                JsonNode v = obj.get(name);
                if (v.isTextual()) {
                    String resolved = resolveString(v.asText());
                    if (!resolved.equals(v.asText())) {
                        obj.set(name, new TextNode(resolved));
                    }
                } else if (v.isContainerNode()) {
                    walk(v);
                }
            }
        } else if (node.isArray()) {
            ArrayNode arr = (ArrayNode) node;
            for (int i = 0; i < arr.size(); i++) {
                JsonNode el = arr.get(i);
                if (el.isTextual()) {
                    String resolved = resolveString(el.asText());
                    if (!resolved.equals(el.asText())) {
                        arr.set(i, new TextNode(resolved));
                    }
                } else if (el.isContainerNode()) {
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
