package com.reddit.label.CsvRowTypes;

public class CsvStringType implements DynamicCsvRowType {
    private String value;

    public CsvStringType(String value) {
        this.value = value;
    }
    
    @Override
    public String stringRepresentation() {

        if (value == null) {
            return "Empty";
        }

        return value;
    }
}
