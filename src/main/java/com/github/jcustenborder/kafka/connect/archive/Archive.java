/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.archive;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Description("The Archive transformation is used to help preserve all of the data for a message when archived to S3.")
@DocumentationNote("This transform works by copying the key, value, topic, and timestamp to new record where this is all " +
        "contained in the value of the message. This will allow connectors like Confluent's S3 connector to properly archive " +
        "the record.")
public class Archive<R extends ConnectRecord<R>> implements Transformation<R> {

  private Cache<String, Schema> schemaUpdateCache;

  @Override
  public R apply(R r) {
    if (r.valueSchema() == null && r.keySchema() == null) {
      return applySchemaless(r);
    } else {
      return applyWithSchema(r);
    }
  }

  private Schema makeUpdatedSchema(Schema keySchema, Schema valueSchema) {
    return SchemaBuilder.struct()
            .name("com.github.jcustenborder.kafka.connect.archive.Storage")
            .field("key", keySchema).optional()
            .field("value", valueSchema).optional()
            .field("topic", Schema.STRING_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA);
  }

  private R applyWithSchema(R r) {
    String cacheKey = String.format("%s-key", r.topic());
    String cacheValue = String.format("%s-value", r.topic());
    Schema cachedKey = schemaUpdateCache.get(cacheKey);
    Schema cachedValue = schemaUpdateCache.get(cacheValue);

    Schema keySchema = r.keySchema();
    if (keySchema == null) {
      keySchema = (cachedKey != null ? cachedKey : Schema.OPTIONAL_STRING_SCHEMA);
    } else {
      if (cachedKey != null) {
        if (schemaUpdateCache.get(cacheKey).version() < keySchema.version()) {
          schemaUpdateCache.put(cacheKey, keySchema);
        }
      } else {
        schemaUpdateCache.put(cacheKey, keySchema);
      }
    }

    Schema valueSchema = r.valueSchema();
    if (valueSchema == null) {
      Schema cached = schemaUpdateCache.get(cacheValue);
      valueSchema = (cached != null ? cached : Schema.OPTIONAL_STRING_SCHEMA);
    } else {
      if (cachedValue != null) {
        if (schemaUpdateCache.get(cacheValue).version() < valueSchema.version()) {
          schemaUpdateCache.put(cacheValue, valueSchema);
        }
      } else {
        schemaUpdateCache.put(cacheValue, valueSchema);
      }
    }

    Schema schema = makeUpdatedSchema(keySchema, valueSchema);
    Struct value = new Struct(schema)
            .put("key", r.key())
            .put("value", r.value())
            .put("topic", r.topic())
            .put("timestamp", r.timestamp());
    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema, value, r.timestamp());
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R r) {

    final Map<String, Object> archiveValue = new HashMap<>();

    final Map<String, Object> value = (Map<String, Object>) r.value();

    archiveValue.put("key", r.key());
    archiveValue.put("value", value);
    archiveValue.put("topic", r.topic());
    archiveValue.put("timestamp", r.timestamp());

    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), null, archiveValue, r.timestamp());
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<String, Schema>(16));
  }
}