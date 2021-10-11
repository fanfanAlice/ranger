package org.apache.ranger.services.kyligence.client.json.model;

import java.util.Locale;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SensitiveDataMask {
    private static final Set<String> VALID_DATA_TYPES = Sets.newHashSet(DataType.STRING, DataType.VARCHAR,
            DataType.CHAR, DataType.INT, DataType.INTEGER, DataType.BIGINT, DataType.SMALL_INT, DataType.TINY_INT,
            DataType.DOUBLE, DataType.FLOAT, DataType.REAL, DataType.DECIMAL, DataType.DATE, DataType.TIMESTAMP,
            DataType.DATETIME);

    public static boolean isValidDataType(String dataType) {
        int parenthesesIdx = dataType.indexOf('(');
        return VALID_DATA_TYPES
                .contains(parenthesesIdx > -1 ? dataType.substring(0, parenthesesIdx).trim().toLowerCase(Locale.ROOT)
                        : dataType.trim().toLowerCase(Locale.ROOT));
    }

    public enum MaskType {
        DEFAULT(0), // mask sensitive data by type with default values
        AS_NULL(1); // mask all sensitive data as NULL

        int priority = 0; // smaller number stands for higher priority

        MaskType(int priority) {
            this.priority = priority;
        }

        public MaskType merge(MaskType other) {
            if (other == null) {
                return this;
            }
            return this.priority < other.priority ? this : other;
        }
    }

    @JsonProperty
    String column;

    @JsonProperty
    MaskType type;

    public SensitiveDataMask() {
    }

    public SensitiveDataMask(String column, MaskType type) {
        this.column = column;
        this.type = type;
    }

    public MaskType getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }
}
