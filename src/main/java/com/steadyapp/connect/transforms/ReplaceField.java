package com.steadyapp.connect.transforms;

//region Imports

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

//endregion


public abstract class ReplaceField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Filter or rename fields."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + ReplaceField.Key.class.getName() + "</code>) "
            + "or value (<code>" + ReplaceField.Value.class.getName() + "</code>).";

    interface ConfigName {
        String EXCLUDE = "exclude";
        String INCLUDE = "include";
        String RENAME = "renames";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.EXCLUDE, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to exclude. This takes precedence over the include.")
            .define(ConfigName.INCLUDE, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to include. If specified, only these fields will be used.")
            .define(ReplaceField.ConfigName.RENAME, ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.Validator() {
                @SuppressWarnings("unchecked")
                @Override
                public void ensureValid(String name, Object value) {
                    parseRenameMappings((List<String>) value);
                }

                @Override
                public String toString() {
                    return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                }
            }, ConfigDef.Importance.MEDIUM, "Field rename mappings.");

    private static final String PURPOSE = "field replacement";

    private List<String> exclude;
    private List<String> include;
    private Map<String, String> renames;
    private Map<String, String> reverseRenames;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        exclude = config.getList(ConfigName.EXCLUDE);
        include = config.getList(ConfigName.INCLUDE);
        renames = parseRenameMappings(config.getList(ReplaceField.ConfigName.RENAME));
        reverseRenames = invert(renames);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    static Map<String, String> parseRenameMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(ReplaceField.ConfigName.RENAME, mappings, "Invalid rename mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
        }
        return m;
    }

    static Map<String, String> invert(Map<String, String> source) {
        final Map<String, String> m = new HashMap<>();
        for (Map.Entry<String, String> e : source.entrySet()) {
            m.put(e.getValue(), e.getKey());
        }
        return m;
    }

    static String join(String parent, String name) {
        if (parent == null || parent.isEmpty()) {
            return name;
        } else {
            return String.format("%s.%s", parent, name);
        }
    }

    boolean filter(String fieldName) {
        return !exclude.contains(fieldName) && (include.isEmpty() || include.contains(fieldName));
    }

    String renamed(String fieldName) {
        final String mapping = renames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    String reverseRenamed(String fieldName) {
        final String mapping = reverseRenames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    private Map<String, Object> expandSchemaless(Object input, String parent) {
        final Map<String, Object> value = requireMap(input, PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = join(parent, e.getKey());
            Object fieldValue = e.getValue();
            if (!(fieldValue instanceof Map)) {
                fieldValue = expandSchemaless(fieldValue, fieldName);
            }
            if (filter(fieldName)) {
                updatedValue.put(renamed(fieldName), fieldValue);
            }
        }

        return updatedValue;
    }

    private Struct expandStruct(Schema schema, Struct value, String parent) {
        final Struct updatedValue = new Struct(schema);
        for (Field field : schema.fields()) {
            final String fieldName = join(parent, field.name());
            Object fieldValue = value.get(reverseRenamed(field.name()));

            if (field.schema().type() == Schema.Type.STRUCT) {
                Struct structValue = requireStruct(fieldValue, PURPOSE);
                fieldValue = expandStruct(field.schema(), structValue, fieldName);
            }
            updatedValue.put(field.name(), fieldValue);
        }

        return updatedValue;
    }


    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> updatedValue = expandSchemaless(operatingValue(record), null);

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), null);
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = expandStruct(updatedSchema, value, null);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema, String parent) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            final String fieldName = join(parent, field.name());
            Schema fieldSchema = field.schema();
            if (!field.schema().type().isPrimitive()) {
                fieldSchema = makeUpdatedSchema(fieldSchema, fieldName);
            }
            if (filter(fieldName)) {
                builder.field(renamed(fieldName), fieldSchema);
            }

        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ReplaceField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ReplaceField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }


}
