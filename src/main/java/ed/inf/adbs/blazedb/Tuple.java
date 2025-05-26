package ed.inf.adbs.blazedb;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Schema-aware tuple implementation.
 * This design simplifies operator implementation while providing stronger type safety.
 */
public class Tuple {
    public final List<Integer> values;
    public final List<String> schema;
    
    /**
     * Creates a new tuple with specified values and schema.
     * 
     * @param values The integer values contained in this tuple
     * @param schema The corresponding column names (table.column format)
     */
    public Tuple(List<Integer> values, List<String> schema) {
        this.values = values;
        this.schema = schema;
    }
    
    /**
     * Retrieves a value by column name.
     * Performs a lookup in the schema to find the correct position.
     * 
     * @param column The fully qualified column name (table.column)
     * @return The corresponding integer value, or null if column not found
     */
    public Integer get(String column) {
        int index = schema.indexOf(column);
        return index != -1 ? values.get(index) : null;
    }

    /**
     * Creates a new tuple by combining values and schemas from two input tuples.
     * Used during join operations to create result tuples.
     * 
     * @param left The left tuple in the join
     * @param right The right tuple in the join
     * @return A new tuple containing all values and schema entries from both input tuples
     */
    public static Tuple join(Tuple left, Tuple right) {
        List<Integer> newValues = new ArrayList<>(left.values);
        newValues.addAll(right.values);
        List<String> newSchema = new ArrayList<>(left.schema);
        newSchema.addAll(right.schema);
        return new Tuple(newValues, newSchema);
    }
    
    /**
     * Compares this tuple with another object for equality.
     * Two tuples are equal if they have the same values and schema.
     * 
     * @param o The object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(values, tuple.values) && 
               Objects.equals(schema, tuple.schema);
    }
    
    /**
     * Generates a hash code for this tuple based on its values and schema.
     * 
     * @return A hash code value for this tuple
     */
    @Override
    public int hashCode() {
        return Objects.hash(values, schema);
    }
    
    /**
     * Converts this tuple to a string representation with comma-separated values.
     * Used for writing query results to output files.
     * 
     * @return A comma-separated string of the tuple's values
     */
    @Override
    public String toString() {
        return values.stream()
            .map(value -> value != null ? value.toString() : "herenull") // Handle null values
            .collect(Collectors.joining(","));
    }
}