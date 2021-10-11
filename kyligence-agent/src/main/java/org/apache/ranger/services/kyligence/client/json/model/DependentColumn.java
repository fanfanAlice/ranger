package org.apache.ranger.services.kyligence.client.json.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DependentColumn {

    @JsonProperty
    String column;

    @JsonProperty("dependent_column_identity")
    String dependentColumnIdentity;

    @JsonProperty("dependent_values")
    String[] dependentValues;

    public DependentColumn() {
    }

    public DependentColumn(String column, String dependentColumnIdentity, String[] dependentValues) {
        this.column = column;
        this.dependentColumnIdentity = dependentColumnIdentity;
        this.dependentValues = dependentValues;
    }

    public String getColumn() {
        return column;
    }

    public String getDependentColumnIdentity() {
        return dependentColumnIdentity;
    }

    public String[] getDependentValues() {
        return dependentValues;
    }

    public DependentColumn merge(DependentColumn other) {
        Preconditions.checkArgument(other != null);
        Preconditions.checkArgument(other.column.equalsIgnoreCase(this.column));
        Preconditions.checkArgument(other.dependentColumnIdentity.equalsIgnoreCase(this.dependentColumnIdentity));
        Set<String> values = Sets.newHashSet(dependentValues);
        values.addAll(Arrays.asList(other.dependentValues));
        return new DependentColumn(column, dependentColumnIdentity, values.toArray(new String[0]));
    }
}
