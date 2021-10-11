package org.apache.ranger.services.kyligence.client.json.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AclTCRResponse {

    @JsonProperty("authorized_table_num")
    private int authorizedTableNum;

    @JsonProperty("total_table_num")
    private int totalTableNum;

    @JsonProperty("database_name")
    private String databaseName;

    @JsonProperty
    private List<Table> tables;

    public int getAuthorizedTableNum() {
        return authorizedTableNum;
    }

    public void setAuthorizedTableNum(int authorizedTableNum) {
        this.authorizedTableNum = authorizedTableNum;
    }

    public int getTotalTableNum() {
        return totalTableNum;
    }

    public void setTotalTableNum(int totalTableNum) {
        this.totalTableNum = totalTableNum;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public static class Table {
        @JsonProperty
        private boolean authorized;

        @JsonProperty("table_name")
        private String tableName;

        @JsonProperty("authorized_column_num")
        private int authorizedColumnNum;

        @JsonProperty("total_column_num")
        private int totalColumnNum;

        @JsonProperty
        private List<Column> columns = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty
        private List<Row> rows = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("like_rows")
        private List<Row> likeRows = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("row_filter")
        private RowFilter rowFilter = new RowFilter();

        public boolean isAuthorized() {
            return authorized;
        }

        public void setAuthorized(boolean authorized) {
            this.authorized = authorized;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public int getAuthorizedColumnNum() {
            return authorizedColumnNum;
        }

        public void setAuthorizedColumnNum(int authorizedColumnNum) {
            this.authorizedColumnNum = authorizedColumnNum;
        }

        public int getTotalColumnNum() {
            return totalColumnNum;
        }

        public void setTotalColumnNum(int totalColumnNum) {
            this.totalColumnNum = totalColumnNum;
        }

        public List<Column> getColumns() {
            return columns;
        }

        public void setColumns(List<Column> columns) {
            this.columns = columns;
        }

        public List<Row> getRows() {
            return rows;
        }

        public void setRows(List<Row> rows) {
            this.rows = rows;
        }

        public List<Row> getLikeRows() {
            return likeRows;
        }

        public void setLikeRows(List<Row> likeRows) {
            this.likeRows = likeRows;
        }

        public RowFilter getRowFilter() {
            return rowFilter;
        }

        public void setRowFilter(RowFilter rowFilter) {
            this.rowFilter = rowFilter;
        }
    }

    public static class Column {
        @JsonProperty
        private boolean authorized;

        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("data_mask_type")
        private SensitiveDataMask.MaskType dataMaskType;

        @JsonProperty("dependent_columns")
        private List<DependentColumnData> dependentColumns;

        @JsonProperty("datatype")
        private String datatype;

        public void setDependentColumns(Collection<DependentColumn> dependentColumns) {
            this.dependentColumns = dependentColumns.stream()
                    .map(col -> new DependentColumnData(col.getDependentColumnIdentity(), col.getDependentValues()))
                    .collect(Collectors.toList());
        }
    }

    public static class Row {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty
        private List<String> items;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public List<String> getItems() {
            return items;
        }

        public void setItems(List<String> items) {
            this.items = items;
        }
    }

    public static class Filter {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("in_items")
        private List<String> inItems = new ArrayList<>();

        @JsonProperty("like_items")
        private List<String> likeItems = new ArrayList<>();
    }

    public static class FilterGroup {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("is_group")
        private boolean group = false;

        @JsonProperty
        private List<AclTCRResponse.Filter> filters = new ArrayList<>();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isGroup() {
            return group;
        }

        public void setGroup(boolean group) {
            this.group = group;
        }

        public List<Filter> getFilters() {
            return filters;
        }

        public void setFilters(List<Filter> filters) {
            this.filters = filters;
        }
    }

    public static class RowFilter {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("filter_groups")
        private List<AclTCRResponse.FilterGroup> filterGroups = new ArrayList<>();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<FilterGroup> getFilterGroups() {
            return filterGroups;
        }

        public void setFilterGroups(List<FilterGroup> filterGroups) {
            this.filterGroups = filterGroups;
        }
    }

    public static class DependentColumnData {
        @JsonProperty("column_identity")
        private String columnIdentity;

        @JsonProperty("values")
        private String[] values;

        public DependentColumnData(String columnIdentity, String[] values) {
            this.columnIdentity = columnIdentity;
            this.values = values;
        }

        public DependentColumnData() {
        }

        public String getColumnIdentity() {
            return columnIdentity;
        }

        public void setColumnIdentity(String columnIdentity) {
            this.columnIdentity = columnIdentity;
        }

        public String[] getValues() {
            return values;
        }

        public void setValues(String[] values) {
            this.values = values;
        }
    }
}
