package org.apache.ranger.services.kyligence.client.json.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class AclTCRRequest {

    @JsonProperty("database_name")
    private String databaseName;

    @JsonProperty
    private List<Table> tables;

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
        @JsonProperty("table_name")
        private String tableName;

        @JsonProperty
        private boolean authorized;

        @JsonProperty
        private List<Column> columns;

        // Default value for rows, like_rows and row_filter is null
        // DO NOT CHANGE TO EMPTY LIST OR EMPTY ROW FILTER
        @JsonProperty
        private List<Row> rows;

        @JsonProperty("like_rows")
        private List<Row> likeRows;

        @JsonProperty("row_filter")
        private RowFilter rowFilter;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public boolean isAuthorized() {
            return authorized;
        }

        public void setAuthorized(boolean authorized) {
            this.authorized = authorized;
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

        @Override
        public String toString() {
            return "Table{" +
                    "tableName='" + tableName + '\'' +
                    ", authorized=" + authorized +
                    ", columns=" + columns +
                    ", rows=" + rows +
                    ", likeRows=" + likeRows +
                    ", rowFilter=" + rowFilter +
                    '}';
        }
    }


    public static class Column {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty
        private boolean authorized;

        @JsonProperty("data_mask_type")
        private SensitiveDataMask.MaskType dataMaskType;

        @JsonProperty("dependent_columns")
        private List<DependentColumnData> dependentColumns;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public boolean isAuthorized() {
            return authorized;
        }

        public void setAuthorized(boolean authorized) {
            this.authorized = authorized;
        }

        public SensitiveDataMask.MaskType getDataMaskType() {
            return dataMaskType;
        }

        public void setDataMaskType(SensitiveDataMask.MaskType dataMaskType) {
            this.dataMaskType = dataMaskType;
        }

        public List<DependentColumnData> getDependentColumns() {
            return dependentColumns;
        }

        public void setDependentColumns(List<DependentColumnData> dependentColumns) {
            this.dependentColumns = dependentColumns;
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

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public List<String> getInItems() {
            return inItems;
        }

        public void setInItems(List<String> inItems) {
            this.inItems = inItems;
        }

        public List<String> getLikeItems() {
            return likeItems;
        }

        public void setLikeItems(List<String> likeItems) {
            this.likeItems = likeItems;
        }
    }

    public static class FilterGroup {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("is_group")
        private boolean group;

        @JsonProperty
        private List<Filter> filters = new ArrayList<>();

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
        private List<FilterGroup> filterGroups = new ArrayList<>();

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

    @Override
    public String toString() {
        return "AclTCRRequest{" +
                "databaseName='" + databaseName + '\'' +
                ", tables=" + tables.toString() +
                '}';
    }
}
