# kafka-producer-common-stub

Config-driven Kafka producer that maps a sample JSON file to an Avro schema and publishes to a topic. Built on **Spring Cloud Stream Kafka binder** so it stays consistent with the rest of the org.

A user only edits `src/main/resources/application.yml`. No code changes needed to add a new topic — just point at another JSON + `.avsc` pair.

## What it does

1. Reads one or more `(topic, json-path, avro-schema-path)` entries from `application.yml`.
2. Loads each `.avsc` and the matching JSON sample.
3. Maps JSON → Avro `GenericRecord` (strict by field name, falls back to Avro defaults / nulls; unknown JSON keys fail loudly).
4. Sends via `StreamBridge` using `KafkaAvroSerializer` + Confluent Schema Registry. Schemas auto-register under the standard `<topic>-value` subject.

## Run modes

| `app.mode` | Behaviour |
|---|---|
| `ONESHOT` | Publishes every configured topic on startup, then exits. Good for seeding. |
| `SERVER` | Stays up; trigger via `POST /publish/{key}` or `POST /publish/{key}/inline`. |

## Prerequisites

- Java 17+
- Maven 3.9+
- A reachable Kafka broker and Confluent-compatible Schema Registry. For local dev, the `confluentinc/cp-kafka` + `cp-schema-registry` Docker images work out of the box.

## Configure your topics

Edit `src/main/resources/application.yml`:

```yaml
app:
  mode: ONESHOT
  records-per-topic: 1
  topics:
    - key: user
      topic: user-events
      json-path: classpath:samples/user.json     # or file:/absolute/path/to.json
      avro-schema-path: classpath:samples/user.avsc
      record-count: 3
      message-key-field: id
```

Cluster + schema registry come from env vars (with localhost defaults):

```bash
export KAFKA_BROKERS=broker1:9092,broker2:9092
export SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

## Run

### One-shot publish (CLI style)

```bash
mvn spring-boot:run
```

The app publishes every topic in `app.topics` and exits.

To override a topic's JSON/schema at the command line:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --app.topics[0].json-path=file:/tmp/my-user.json \
  --app.topics[0].avro-schema-path=file:/tmp/my-user.avsc"
```

### Server mode

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--app.mode=SERVER"
```

Then:

```bash
# Publish the configured sample 5 times to topic "user-events"
curl -X POST "http://localhost:8080/publish/user?count=5"

# Publish a custom JSON body against the configured schema for key "user"
curl -X POST http://localhost:8080/publish/user/inline \
  -H "Content-Type: application/json" \
  -d '{"id":"u-99","name":"Bob","email":"b@x.com","age":42,"createdAt":1716470400000,"tags":["beta"]}'
```

## Adding a new topic

1. Drop `your-topic.avsc` into `src/main/resources/samples/` (or anywhere on the filesystem).
2. Drop a matching `your-topic.json` next to it.
3. Add an entry under `app.topics` pointing at both files and giving the Kafka topic name.
4. Run.

No Java changes required.

## Dynamic values in sample JSON

The sample JSON files support `${...}` placeholders that are re-evaluated **on every send** (so each record gets a fresh value, not the same value reused N times):

| Placeholder | Replaced with |
|---|---|
| `${uuid}` | `UUID.randomUUID()` |
| `${now.millis}` | current epoch millis |
| `${now.iso}` | current `Instant` (ISO-8601) |

Unknown placeholders pass through unchanged. Example:

```json
{ "id": "${uuid}", "createdAt": "${now.millis}", "name": "Alice" }
```

To add more (e.g. `${random.int}`), extend the `switch` in `JsonPlaceholderResolver`.

## Avro logical types

Human-readable JSON values are accepted for these logical types and converted to the underlying primitive automatically (the schema's `logicalType` tag is preserved on the wire):

| Logical type | Underlying | Accepted JSON |
|---|---|---|
| `date` | `int` (days since epoch) | `"2026-01-01"` or full ISO timestamp (UTC date portion) or integer days |
| `time-millis` | `int` | `"14:30:00"` or integer millis |
| `time-micros` | `long` | `"14:30:00"` or integer micros |
| `timestamp-millis` | `long` | `"2026-01-01T00:00:00Z"` or integer epoch millis |
| `timestamp-micros` | `long` | ISO instant or integer epoch micros |
| `local-timestamp-millis` | `long` | `"2026-01-01T00:00:00"` or integer |
| `local-timestamp-micros` | `long` | `"2026-01-01T00:00:00"` or integer |
| `uuid` | `string` | any string |

The sample schema demonstrates `date` (`birthDate`) and `timestamp-millis` (`createdAt`).

## Mapping rules (strict + defaults)

- Field names in the JSON must match the Avro schema field names exactly.
- Primitive coercion happens for unambiguous cases (numeric strings → `int`/`long`/`double`, `"true"`/`"false"` → `boolean`).
- Missing JSON field with an Avro `default` → uses the default.
- Missing JSON field on a `["null", T]` union → writes `null`.
- Missing JSON field that is required and has no default → fails with a clear error.
- JSON key not present in the Avro schema → fails (catches typos and schema drift early).
- Unions: the first non-null branch whose JSON shape matches is used.

## Production-style auth (TODO)

The stub ships with plaintext defaults. To target a SASL/SSL cluster, uncomment the `configuration:` block under `spring.cloud.stream.kafka.binder` in `application.yml` and set `KAFKA_USER` / `KAFKA_PASSWORD`.

## Build

```bash
mvn clean package
java -jar target/kafka-producer-common-stub-0.1.0-SNAPSHOT.jar
```
