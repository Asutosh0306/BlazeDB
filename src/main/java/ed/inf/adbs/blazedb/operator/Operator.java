package ed.inf.adbs.blazedb.operator;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.util.*;
import java.util.stream.Collectors;

import ed.inf.adbs.blazedb.BlazeDB;
import ed.inf.adbs.blazedb.Tuple;

import java.util.function.BiPredicate;

import java.io.*;



/**
 * Base class for the iterator model implementation of relational operators.
 * Defines the interface for tuple-by-tuple processing of query plans.
 */
public abstract class Operator {
    
	/**
	 * Physical operator that reads tuples directly from a data file.
	 * Represents the leaf nodes in the operator tree and provides base table access.
	 * Supports projection pushdown by loading only required columns.
	 */
    public static class ScanOperator extends Operator {
        private final String table;
        private final List<String> schema;
        private final List<Integer> requiredIndices; // Indices of required columns
        private BufferedReader reader;
        private final String databaseDir;
        
        /**
         * Creates a new scan operator for the specified table.
         * 
         * @param table The name of the table to scan
         * @param catalog The database catalog with schema information
         * @param databaseDir The directory containing data files
         * @param requiredColumns Set of columns needed by the query (for projection pushdown)
         */
        public ScanOperator(String table, BlazeDB.Catalog catalog,String databaseDir,Set<String> requiredColumns) {
            this.table = table;
            
            // Get full schema from catalog
            List<String> fullSchema = catalog.getColumns(table).stream()
                .map(col -> table + "." + col)
                .collect(Collectors.toList());
            
            // Filter required columns for this table and build schema
            this.schema = requiredColumns.stream()
                .filter(col -> col.startsWith(table + "."))
                .collect(Collectors.toList());
            
            // Map required columns to indices in the full schema
            this.requiredIndices = this.schema.stream()
                .map(col -> fullSchema.indexOf(col))
                .filter(idx -> idx != -1)
                .collect(Collectors.toList());
            
            this.databaseDir = databaseDir;
            //System.out.println("[SCAN] Table: " + table + " | Columns: " + schema);
            reset();
        }

        @Override public Tuple getNextTuple() {
            try {
            	
                String line = reader.readLine();
                //System.out.println("Reading: ");
                //System.out.println(line);
                return line != null ? parseLine(line) : null;
            } catch (IOException e) { return null; }
        }
        
        /**
         * Parses a line from the CSV file into a tuple.
         * Applies projection pushdown by extracting only the required columns.
         * 
         * @param line A line from the CSV file representing one tuple
         * @return A tuple containing the parsed values with proper schema information
         */
        private Tuple parseLine(String line) {
        	String[] allValues = line.split(",");
            List<Integer> values = requiredIndices.stream()
                .map(idx -> Integer.parseInt(allValues[idx].trim()))
                .collect(Collectors.toList());
            //System.out.println("[SCAN DEBUG] | Value: " + values);
            return new Tuple(values, schema);
        }

        @Override public void reset() {
            try {
            	// Construct full path: databaseDir + "/data/" + table + ".csv"
                String dataDir = databaseDir + File.separator + "data";
                String filePath = dataDir + File.separator + table + ".csv";
                reader = new BufferedReader(new FileReader(filePath));
            } catch (FileNotFoundException e) { throw new RuntimeException(e); }
        }
        
        public String getTable() {
            return table;
        }
    }

    
    /**
     * Filtering operator that implements the relational algebra selection operation.
     * Passes through only tuples that satisfy the selection condition.
     */
    public static class SelectOperator extends Operator {
        private final Operator child;
        private final Expression condition;
        private final ExpressionEvaluator evaluator = new ExpressionEvaluator();
        
        /**
         * Creates a new selection operator with the specified condition.
         * 
         * @param child The child operator providing input tuples
         * @param condition The expression used to filter tuples
         */
        public SelectOperator(Operator child, Expression condition) {
            this.child = child;
            this.condition = condition;
        }

        @Override public Tuple getNextTuple() {
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
            	//System.out.println("Select tuple: "+tuple);
            	if (evaluator.evaluate(condition, tuple) != 0) {  // Treat non-zero as true
            	    return tuple;
            	}
                //if (evaluator.evaluate(condition, tuple)) return tuple;
            }
            //System.out.println("Select tuple: "+tuple);
            return null;
        }

        @Override public void reset() { child.reset(); }
        
        public Operator getChild() {
            return child;
        }
    }

    
    /**
     * Projection operator that implements the relational algebra projection operation.
     * Creates new tuples containing only the specified columns from input tuples.
     */
    public static class ProjectOperator extends Operator {
        private final Operator child;
        private final List<String> projection;
        
        /**
         * Creates a new projection operator with the specified output columns.
         * 
         * @param child The child operator providing input tuples
         * @param projection List of column names to include in the output
         */
        public ProjectOperator(Operator child, List<String> projection) {
            this.child = child;
            this.projection = projection;
        }

        @Override 
        public Tuple getNextTuple() {
            Tuple tuple = child.getNextTuple();
            //System.out.println("Child: "+child);
            //System.out.println("Projection: "+projection);
            //System.out.println("Project tuple: "+tuple);
            return tuple != null ? project(tuple) : null;
        }

        
        /**
         * Creates a projected tuple from an input tuple by extracting specified columns.
         * 
         * @param inputTuple The input tuple to project
         * @return A new tuple containing only the projected columns
         */
        private Tuple project(Tuple inputTuple) {
            List<Integer> projectedValues = new ArrayList<>();
            List<String> projectedSchema = new ArrayList<>();
            
            //System.out.println("[PROJECT] Input Schema: " + inputTuple.schema);
            //System.out.println("[PROJECT] Projection List: " + projection); 
            

            for (String columnName : projection) {
                Integer value;
                
                // Handle aggregate columns from SumOperator
                if (columnName.startsWith("SUM(")) {
                    // Aggregates are at the end of the tuple's values
                    int aggPosition = inputTuple.schema.indexOf(columnName);
                    value = aggPosition != -1 ? inputTuple.values.get(aggPosition) : null;
                } else {
                    // Regular column lookup
                    value = inputTuple.get(columnName);
                }

                if (value != null) {
                    projectedValues.add(value);
                    projectedSchema.add(columnName);
                } else {
                    System.err.println("Column '" + columnName + "' not found in tuple.");
                    projectedValues.add(null);
                    projectedSchema.add(columnName);
                }
            }

            //System.out.println("[PROJECT] Output Values: " + projectedValues);

            return new Tuple(projectedValues, projectedSchema);
        }





        @Override public void reset() { child.reset(); }
    }
    
    
    /**
     * Join operator that implements a nested loop join algorithm.
     * Combines tuples from two input operators based on an optional join condition.
     */
    public static class JoinOperator extends Operator {
        private Operator left, right;
        private final Expression condition;
        private Tuple currentLeft;
        private final ExpressionEvaluator evaluator = new ExpressionEvaluator();
        
        /**
         * Creates a new join operator with the specified inputs and condition.
         * 
         * @param left The left (outer) child operator 
         * @param right The right (inner) child operator
         * @param condition The optional join condition, or null for cross product
         */
        public JoinOperator(Operator left, Operator right, Expression condition) {
            this.left = left;
            this.right = right;
            this.condition = condition;
            reset();
        }

        @Override public Tuple getNextTuple() {
            while (true) {
                if (currentLeft == null) {
                    currentLeft = left.getNextTuple();
                    right.reset();
                    if (currentLeft == null) return null;
                }
                
                Tuple rightTuple = right.getNextTuple();
                if (rightTuple == null) {
                    currentLeft = null;
                    continue;
                }
                
                Tuple joined = Tuple.join(currentLeft, rightTuple);
                if (condition == null || evaluator.evaluate(condition, joined) != 0) {
                    return joined;
//                if (condition == null || evaluator.evaluate(condition, joined)) {
//                    return joined;
                }
            }
        }

        @Override public void reset() {
            left.reset();
            right.reset();
            currentLeft = null;
        }
        
        /**
         * Sets a new left child operator.
         * Used during query optimization for predicate pushdown.
         * 
         * @param left The new left child operator
         */
        public void setLeft(Operator left) { 
        	this.left = left; 
        }
        
        /**
		 * Sets a new right child operator.
		 * Used during query optimization for predicate pushdown.
		 * 
		 * @param right The new right child operator
		 */
		public void setRight(Operator right) {
		    this.right = right;
		}
        
        /**
         * Gets the current left child operator.
         * 
         * @return The left child operator
         */
		public Operator getLeft() {
			return left;
		}
		
		/**
		 * Gets the current right child operator.
		 * 
		 * @return The right child operator
		 */
		public Operator getRight() {
		    return right;
		}

    }

    
    
    /**
     * Evaluates SQL expressions against tuples using the visitor pattern.
     * Handles comparison operators, logical operators, column references, and literals.
     */
    private static class ExpressionEvaluator extends ExpressionVisitorAdapter {
        private Tuple tuple;
        //private boolean result;
        private final Map<String, Integer> values = new HashMap<>();
        
        /**
         * Evaluates an expression against a tuple.
         * 
         * @param expr The expression to evaluate
         * @param tuple The tuple to evaluate against
         * @return Non-zero (here typically 1) if the expression evaluates to true, 0 otherwise
         */
        public int evaluate(Expression expr, Tuple tuple) {
            this.tuple = tuple;
            expr.accept(this);
            //return result;
            int results=values.getOrDefault("temp",0);
            //System.out.println("[EVAL] Evaluated " + expr + " => " + results);
            return results;
        }

        // Comparison handlers
        @Override
        public void visit(EqualsTo expr) {
            handleComparison(expr, (a, b) -> a == b);
        }

        @Override
        public void visit(NotEqualsTo expr) {
            handleComparison(expr, (a, b) -> a != b);
        }

        @Override
        public void visit(GreaterThan expr) {
            handleComparison(expr, (a, b) -> a > b);
        }

        @Override
        public void visit(MinorThan expr) {
            handleComparison(expr, (a, b) -> a < b);
        }

        @Override
        public void visit(GreaterThanEquals expr) {
            handleComparison(expr, (a, b) -> a >= b);
        }

        @Override
        public void visit(MinorThanEquals expr) {
            handleComparison(expr, (a, b) -> a <= b);
        }

        /**
         * Handles evaluation of comparison expressions like equals, less than, etc.
         * 
         * @param expr The binary comparison expression
         * @param predicate The comparison logic to apply
         */
        private void handleComparison(BinaryExpression expr, BiPredicate<Integer, Integer> predicate) {
            expr.getLeftExpression().accept(this);
            int left = values.get("temp");
            expr.getRightExpression().accept(this);
            int right = values.get("temp");
            values.put("temp", predicate.test(left, right) ? 1 : 0);
        }

        // Value handlers
        @Override
        public void visit(Column column) {
            String fullName = column.getTable().getName() + "." + column.getColumnName();
            values.put("temp", tuple.get(fullName));
        }

        @Override
        public void visit(LongValue value) {
            values.put("temp", (int) value.getValue());
        }

        // Logical operators
        @Override
        public void visit(AndExpression expr) {
        	 expr.getLeftExpression().accept(this);
             int left = values.get("temp");
             expr.getRightExpression().accept(this);
             int right = values.get("temp");
             values.put("temp", (left != 0 && right != 0) ? 1 : 0);
        }
        
     // Multiplication handling
        @Override
        public void visit(Multiplication multiplication) {
            multiplication.getLeftExpression().accept(this);
            int left = values.get("temp");
            multiplication.getRightExpression().accept(this);
            int right = values.get("temp");
            values.put("temp", left * right);
        }
        
        
    }
    
    /**
     * Sort operator that implements the ORDER BY clause.
     * Buffers all input tuples, sorts them, and provides access to the sorted results.
     */
    public static class SortOperator extends Operator {
        private final Operator child;
        private final List<Tuple> buffer = new ArrayList<>();
        private int cursor = 0;
        private final List<String> sortColumns;
        private final Comparator<Tuple> comparator;
        
        /**
         * Creates a new sort operator with the specified sort columns.
         * Initializes the operator and immediately loads and sorts all data from the child.
         * 
         * @param child The child operator providing input tuples
         * @param sortColumns List of column names to sort by, in priority order
         */
        public SortOperator(Operator child, List<String> sortColumns) {
            this.child = child;
            this.sortColumns = sortColumns;
            this.comparator = createComparator();
            loadAndSort();
        }
        
        /**
         * Creates a comparator for sorting tuples based on the specified sort columns.
         * Implements multi-column sorting by comparing columns in the order specified.
         * 
         * @return A comparator that orders tuples according to sort column specifications
         */
        private Comparator<Tuple> createComparator() {
            return (t1, t2) -> {
                for (String col : sortColumns) {
                    int val1 = t1.get(col);
                    int val2 = t2.get(col);
                    if (val1 != val2) {
                        return Integer.compare(val1, val2);
                    }
                }
                return 0;
            };
        }
        
        /**
         * Loads all tuples from the child operator and sorts them.
         * This is a blocking operation that materializes the entire result set in memory.
         */
        private void loadAndSort() {
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                buffer.add(tuple);
            }
            buffer.sort(comparator);
        }

        @Override
        public Tuple getNextTuple() {
            return cursor < buffer.size() ? buffer.get(cursor++) : null;
        }

        @Override
        public void reset() {
            cursor = 0;
        }
    }
    
    /**
     * Duplicate elimination operator that implements the DISTINCT clause.
     * Removes duplicate tuples from the result set.
     */
    public static class DuplicateEliminationOperator extends Operator {
        private final Operator child;
        private final LinkedHashSet<Tuple> seenTuples = new LinkedHashSet<>();
        private Iterator<Tuple> iterator;
        
        /**
         * Creates a new duplicate elimination operator.
         * Immediately loads and de-duplicates all tuples from the child.
         * 
         * @param child The child operator providing input tuples
         */
        public DuplicateEliminationOperator(Operator child) {
            this.child = child;
            loadUniqueTuples();
        }
        
        /**
         * Loads all unique tuples from the child operator.
         * Uses a LinkedHashSet for O(1) duplicate detection while preserving input order.
         * This is a blocking operation that materializes the entire result set in memory.
         */
        private void loadUniqueTuples() {
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                seenTuples.add(tuple);
            }
            iterator = seenTuples.iterator();
        }

        @Override
        public Tuple getNextTuple() {
            return iterator.hasNext() ? iterator.next() : null;
        }

        @Override
        public void reset() {
            iterator = seenTuples.iterator();
        }
    }
    
    /**
     * Aggregation operator that implements SUM and GROUP BY.
     * Groups input tuples and computes sum aggregates for each group.
     */
    public static class SumOperator extends Operator {
        private final Operator child;
        private final List<String> groupByColumns;
        private final List<Expression> sumExpressions;
        private Iterator<Map.Entry<List<Integer>, List<Integer>>> resultIterator;
        private List<String> sumAliases; // Store aliases for SUM expressions
        
        /**
         * Creates a new sum aggregation operator with specified grouping columns and sum expressions.
         * Immediately processes all input tuples and computes aggregations.
         * 
         * @param child The child operator providing input tuples
         * @param groupByColumns List of columns to group by, or empty list for global aggregation
         * @param sumExpressions List of expressions to compute SUM for each group
         */
        public SumOperator(Operator child, List<String> groupByColumns, List<Expression> sumExpressions) {
            this.child = child;
            this.groupByColumns = groupByColumns;
            this.sumExpressions = sumExpressions;
            this.sumAliases = createSumAliases();
            processTuples();
        }
        
        /**
         * Creates column aliases for sum expressions to be used in output schema.
         * 
         * @return List of formatted column names for sum expressions
         */
        private List<String> createSumAliases() {
        	return sumExpressions.stream()
        	        .map(expr -> "SUM(" + expr.toString() + ")") // Match SELECT clause format
        	        .collect(Collectors.toList());
        }
        
        private void processTuples() {
            Map<List<Integer>, List<Integer>> groups = new HashMap<>();
            ExpressionEvaluator evaluator = new ExpressionEvaluator();

            // Handle global aggregation (no GROUP BY)
            List<Integer> globalKey = groupByColumns.isEmpty() 
                ? Collections.singletonList(0) // Dummy key for global aggregation
                : null;
            
            

            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                List<Integer> groupKey = groupByColumns.isEmpty() 
                    ? globalKey 
                    : groupByColumns.stream()
                        .map(tuple::get)
                        .collect(Collectors.toList());
                
                //System.out.println("[SUM OP] Processing tuple: " + tuple.values  + " | Group Key: " + groupKey);

                List<Integer> sums = groups.computeIfAbsent(groupKey, 
                    k -> new ArrayList<>(Collections.nCopies(sumExpressions.size(), 0)));

                for (int i = 0; i < sumExpressions.size(); i++) {
                    int value = evaluator.evaluate(sumExpressions.get(i), tuple);
                    sums.set(i, sums.get(i) + value);
                    //System.out.println("[SUM OP] SUM["+i+"] += " + value + " | Total: " + sums.get(i));
                }
            }

            // Handle empty input for global aggregation
            if (groupByColumns.isEmpty() && groups.isEmpty()) {
                groups.put(globalKey, sumExpressions.stream()
                    .map(expr -> 0)
                    .collect(Collectors.toList()));
            }
            
            //System.out.println("[SUM OP] Final groups: " + groups);

            this.resultIterator = groups.entrySet().iterator();
        }

        @Override
        public Tuple getNextTuple() {
            if (resultIterator.hasNext()) {
                Map.Entry<List<Integer>, List<Integer>> entry = resultIterator.next();
                List<Integer> values = groupByColumns.isEmpty() 
                        ? entry.getValue() 
                        : new ArrayList<>(entry.getKey());
                values.addAll(entry.getValue());
                return new Tuple(values, createOutputSchema());
            }
            return null;
        }
        
        /**
         * Creates the output schema for result tuples.
         * Includes grouping columns (if any) followed by sum aggregate columns.
         * 
         * @return List of column names in the output schema
         */
        private List<String> createOutputSchema() {
        	
        	List<String> schema = new ArrayList<>();
            if (!groupByColumns.isEmpty()) {
                schema.addAll(groupByColumns);
            }
            schema.addAll(sumAliases);
            //System.out.println("[SUM OP] Output Schema: " + schema); 
            return schema;
            
        }

        @Override
        public void reset() {
            processTuples();
        }
    }
   

    /**
     * Retrieves the next tuple from this operator's output.
     * Implements the core of the iterator model, allowing consumers
     * to pull results one tuple at a time.
     * 
     * @return The next output tuple, or null if no more tuples are available
     */
    public abstract Tuple getNextTuple();
    
    // Resets this operator to its initial state.
    public abstract void reset();
}