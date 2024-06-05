package com.reddit.label.CsvRowTypes;

public class CsvIntegerType implements DynamicCsvRowType {
    private Integer value;

    public CsvIntegerType(Integer value) {
        this.value = value;
    }

    @Override
    public String stringRepresentation() {
        if (value == null) {
            return "Empty";
        }

        return value.toString(); 
    }
}
