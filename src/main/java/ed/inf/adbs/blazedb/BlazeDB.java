package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import ed.inf.adbs.blazedb.operator.Operator;

/**
 * Main database class that implements SQL query processing using the iterator model.
 * Provides functionality to parse SQL queries, build optimized query plans and execute queries against data files.
 */
public class BlazeDB {
    
	/**
	 * Singleton catalog that maintains database schema information.
	 * Provides methods to access table schemas and column metadata required for query processing and optimization.
	 */
    public static class Catalog {
        private static Catalog instance;
        // A Hashmap to store the table and column names
        private Map<String, List<String>> schemas = new HashMap<>();
        
        public static Catalog getInstance() {
            if (instance == null) instance = new Catalog();
            return instance;
        }
        /**
         * Initializes the catalog by reading schema definitions from the schema.txt file.
         * Creates a mapping between table names and their column definitions.
         * 
         * @param dbDir The directory path containing the schema.txt file
         * @throws IOException If schema file cannot be read or parsed
         */
        public void initialize(String dbDir) throws IOException {
            try (BufferedReader reader = new BufferedReader(new FileReader(dbDir + File.separator + "schema.txt"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" ");
                    schemas.put(parts[0], Arrays.asList(Arrays.copyOfRange(parts, 1, parts.length)));
                    ////System.out.println("Schemas: "+schemas);
                }
            }
        }
        /**
         * Returns the list of columns for a specified table.
         * 
         * @param table The name of the table to get columns for
         * @return List of column names for the specified table
         */
        public List<String> getColumns(String table) {
            return schemas.get(table);
        }
    }

    /**
     * Entry point for the BlazeDB application.
     * Processes command-line arguments, initializes the database catalog,
     * parses the input SQL query, builds an optimized query plan,
     * and executes the plan to generate query results.
     * 
     * @param args Command-line arguments array containing:
     *             database_dir: The directory containing the database files
     *             input_file: The file path to the SQL query to be processed
     *             output_file: The file path where query results will be saved
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: BlazeDB database_dir input_file output_file");
            return;
        }

        try {
            Catalog catalog = Catalog.getInstance();
            catalog.initialize(args[0]);

            Statement statement = CCJSqlParserUtil.parse(new FileReader(args[1]));
            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select;
            
            String databaseDir = args[0];

            Operator root = buildOptimizedPlan(plainSelect, catalog, databaseDir);
            execute(root, args[2]);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Builds an optimized query execution plan from a SQL PlainSelect statement.
     * Implements several query optimization techniques:
     * 1. Projection pushdown (only reads required columns)
     * 2. Predicate pushdown (filters rows as early as possible)
     * 3. Join condition extraction (evaluates join conditions during joins)
     * 4. Proper operator ordering (aggregation, projection, sorting, distinct)
     * 
     * @param plainSelect The parsed SQL SELECT statement
     * @param catalog The database catalog with schema information
     * @param databaseDir The directory containing database files
     * @return The root operator of the optimized query plan
     * @throws JSQLParserException If there's an error parsing SQL expressions
     */
    private static Operator buildOptimizedPlan(PlainSelect plainSelect, Catalog catalog, String databaseDir) throws JSQLParserException {
        // Process FROM clause (left-deep joins)
        List<String> tables = extractTables(plainSelect);
        //Operator current = buildJoinTree(tables, catalog, databaseDir, plainSelect.getWhere());
        
        // Get all required columns using existing functions
        Set<String> requiredColumns = collectRequiredColumns(plainSelect);

        // Build left-deep joins with projection at scans
        Operator current = buildJoinTree(tables, catalog, databaseDir, plainSelect.getWhere(), requiredColumns);
        
        // Push down remaining predicates
        current = pushDownPredicates(current, plainSelect.getWhere());
        
        
        
        // Add aggregation if needed //aggregation before projection(including global aggregation without GROUP BY)
        boolean hasAggregates = !extractSumExpressions(plainSelect).isEmpty();
        if (plainSelect.getGroupBy() != null || hasAggregates) {
            List<String> groupByColumns = plainSelect.getGroupBy() != null 
                ? extractGroupByColumns(plainSelect) 
                : Collections.emptyList();
            List<Expression> sumExpressions = extractSumExpressions(plainSelect);
            current = new Operator.SumOperator(current, groupByColumns, sumExpressions);
        }
        
        // Add projection
        current = new Operator.ProjectOperator(current, getOutputColumns(plainSelect));
        //System.out.println(current);
        
        // Add sorting if needed
        if (plainSelect.getOrderByElements() != null) {
            List<String> orderColumns = plainSelect.getOrderByElements().stream()
                .map(ob -> ob.getExpression().toString())
                .collect(Collectors.toList());
            current = new Operator.SortOperator(current, orderColumns);
        }
        
        // Add DISTINCT if needed
        if (plainSelect.getDistinct() != null) {
            current = new Operator.DuplicateEliminationOperator(current);
        }
        
        
        return current;
    }

    
    // Helper methods below
    
    
    /**
     * Collects all columns required for query processing based on the SELECT, WHERE, GROUP BY, and ORDER BY clauses. Used for projection pushdown optimization.
     * 
     * @param plainSelect The parsed SQL SELECT statement
     * @return Set of fully qualified column names (table.column) needed for the query
     * @throws JSQLParserException If there's an error accessing SQL expressions
     */
    private static Set<String> collectRequiredColumns(PlainSelect plainSelect) throws JSQLParserException {
        Set<String> columns = new HashSet<>();
        
        // 1. Handle SELECT *
        boolean isSelectAll = plainSelect.getSelectItems().stream()
            .anyMatch(item -> item.getExpression() instanceof AllColumns);
        
        if (isSelectAll) {
            // Add all columns of all tables in FROM
            List<String> tables = extractTables(plainSelect);
            for (String table : tables) {
                List<String> tableColumns = Catalog.getInstance().getColumns(table);
                tableColumns.stream()
                    .map(col -> table + "." + col)
                    .forEach(columns::add);
            }
        } 
        
        else {

	        // 1. SELECT clause: Collect columns from all expressions (including aggregates)
	        for (SelectItem<?> item : plainSelect.getSelectItems()) {
	            item.getExpression().accept(new ColumnCollectorVisitor(columns));
	        }
	
	        // 2. WHERE clause
	        if (plainSelect.getWhere() != null) {
	            plainSelect.getWhere().accept(new ColumnCollectorVisitor(columns));
	        }
	
	        // 3. GROUP BY clause
	        if (plainSelect.getGroupBy() != null) {
	            columns.addAll(extractGroupByColumns(plainSelect));
	        }
	
	        // 4. ORDER BY clause
	        if (plainSelect.getOrderByElements() != null) {
	            plainSelect.getOrderByElements().forEach(obe ->
	                obe.getExpression().accept(new ColumnCollectorVisitor(columns))
	            );
	        }
	
	        // 5. Ensure all tables in FROM have at least one column
	        List<String> tables = extractTables(plainSelect);
	        for (String table : tables) {
	            boolean hasColumns = columns.stream().anyMatch(col -> col.startsWith(table + "."));
	            if (!hasColumns) {
	                // Add ONLY THE FIRST COLUMN of this table
	                List<String> tableColumns = Catalog.getInstance().getColumns(table);
	                if (!tableColumns.isEmpty()) {
	                    String firstColumn = table + "." + tableColumns.get(0); // e.g., "Student.A"
	                    columns.add(firstColumn);
	                }
	            }
	        }
        }

        //System.out.println("[Pro Push] Required columns: " + columns);
        return columns;
    }
    
    /**
     * Expression visitor that collects column references from SQL expressions.
     * Used to identify which columns are needed for query processing. Used for Projection Pushdown optimization.
     */
    private static class ColumnCollectorVisitor extends ExpressionVisitorAdapter {
        private final Set<String> columns;
        public ColumnCollectorVisitor(Set<String> columns) { this.columns = columns; }

        @Override
        public void visit(Column column) {
            columns.add(column.getFullyQualifiedName());
        }

        @Override
        public void visit(Function function) {
            // Handle parameters directly via ExpressionList iteration
            ExpressionList<?> params = function.getParameters();
            if (params != null) {
                params.forEach(expr -> expr.accept(this));
            }
        }
    }

    /**
     * Extracts a list of table names from the FROM clause of a SQL query.
     * 
     * @param plainSelect The parsed SQL SELECT statement
     * @return List of table names in the order they appear in the FROM clause
     */
    private static List<String> extractTables(PlainSelect plainSelect) {
        List<String> tables = new ArrayList<>();
        //gets 1st table
        tables.add(plainSelect.getFromItem().toString());
        //gets all other tables if there is a join
        if (plainSelect.getJoins() != null) {
            plainSelect.getJoins().forEach(j -> tables.add(j.getRightItem().toString()));
        }
        //System.out.println("List of tables: "+tables);
        return tables;
    }

    /**
     * Builds a left-deep join tree based on the tables in the FROM clause.
     * Creates scan operators for base tables and connects them with join operators.
     * 
     * @param tables List of table names from the FROM clause
     * @param catalog The database catalog with schema information
     * @param databaseDir Directory containing the data files
     * @param where WHERE clause expression for extracting join conditions
     * @param requiredColumns Set of columns needed for the query (for projection pushdown)
     * @return Root operator of the constructed join tree
     */
    private static Operator buildJoinTree(List<String> tables, Catalog catalog, String databaseDir, Expression where, Set<String> requiredColumns) {
        Operator current = new Operator.ScanOperator(tables.get(0), catalog, databaseDir, requiredColumns);
        for (int i = 1; i < tables.size(); i++) {
            Operator right = new Operator.ScanOperator(tables.get(i), catalog, databaseDir, requiredColumns);
            current = new Operator.JoinOperator(current, right, extractJoinCondition(where, tables.subList(0, i+1)));
        }
        return current;
    }

    /**
     * Extracts join conditions from the WHERE clause that involve the specified tables.
     * Used to push join conditions into the join operators.
     * 
     * @param where The WHERE clause expression
     * @param tables List of tables to extract join conditions for
     * @return An expression representing the join condition, or null if none exists
     */
    private static Expression extractJoinCondition(Expression where, List<String> tables) {
        TablesNamesFinder finder = new TablesNamesFinder();
        if (where == null) return null; // Handle no WHERE clause
        //where clause broken to individual predicates
        return splitConjunctivePredicates(where).stream()
            .filter(expr -> {
                Set<String> exprTables = finder.getTables(expr); 
                //System.out.println("Extract join conditions tables: "+exprTables);
                return exprTables.size() > 1 && tables.containsAll(exprTables);
            })
            .reduce((a, b) -> new AndExpression(a, b))
            .orElse(null);
    }
    
    /**
     * Splits a complex WHERE clause with AND operators into individual predicates.
     * Used for predicate pushdown optimization.
     * 
     * @param expr The WHERE clause expression to split
     * @return List of individual predicate expressions
     */
    private static List<Expression> splitConjunctivePredicates(Expression expr) {
        List<Expression> predicates = new ArrayList<>();
        Queue<Expression> queue = new LinkedList<>();
        if (expr == null) return predicates; // Handle null input
        queue.add(expr);
        
        //System.out.println("[OPTIMIZER] Starting to split predicates from: " + expr);
        
        while (!queue.isEmpty()) {
            Expression current = queue.poll();
          //gets left and right expressions from and operator
            if (current instanceof AndExpression) {
                queue.add(((AndExpression) current).getLeftExpression());
                queue.add(((AndExpression) current).getRightExpression());
            } else {
                predicates.add(current);
                //System.out.println("[OPTIMIZER] Extracted predicate: " + current);
            }
        }
        return predicates;
    }
    
    
    /**
     * Pushes selection predicates as far down the operator tree as possible.
     * This optimization reduces the number of tuples processed by higher operators.
     * 
     * @param root The root operator of the current query plan
     * @param where The WHERE clause expression containing predicates
     * @return Root operator of the modified plan with pushed-down predicates
     */
    private static Operator pushDownPredicates(Operator root, Expression where) {
        if (where == null) return root;
        final Operator currentRoot[] = {root}; // Mutable reference
        //splitting and temporarily storing in pred and finally currentRoot
        splitConjunctivePredicates(where).forEach(pred -> 
            currentRoot[0] = pushPredicate(currentRoot[0], pred)
        );
        return currentRoot[0];
    }
    

    /**
     * Pushes a single predicate down into the appropriate operator in the tree.
     * For single-table predicates, creates a SelectOperator just above the scan.
     * 
     * @param node Current operator node in the tree
     * @param pred The predicate to push down
     * @return Modified operator with the predicate applied at the appropriate level
     */
    private static Operator pushPredicate(Operator node, Expression pred) {
        if (node instanceof Operator.ScanOperator) {
        	//System.out.println("pustPredicate expressions(Scan): "+pred);
            // Check if predicate references only one table
        	TablesNamesFinder finder = new TablesNamesFinder();
        	Set<String> tables = finder.getTables(pred);
        	//System.out.println("[PUSH-DOWN] Considering pushing predicate: " + pred + " to scan operator for table: " + ((Operator.ScanOperator)node).getTable());
            if (tables.size() == 1) {
                return new Operator.SelectOperator(node, pred); // Push to scan
            }
        }
        if (node instanceof Operator.JoinOperator) {
            // Push to left child (modify recursively)
        	//System.out.println("pustPredicate expressions(Join): "+pred);
        	Operator.JoinOperator join = (Operator.JoinOperator) node;
        	// Find which tables are referenced in the predicate
            TablesNamesFinder finder = new TablesNamesFinder();
            Set<String> predTables = finder.getTables(pred);
            
            // Find which tables are in the left subtree
            Set<String> leftTables = findTablesInOperator(join.getLeft());
            
            // Find which tables are in the right subtree
            Set<String> rightTables = findTablesInOperator(join.getRight());
            
            //System.out.println("[PUSH-DOWN] Analyzing predicate: " + pred);
            //System.out.println("[PUSH-DOWN] Tables in predicate: " + predTables);
            //System.out.println("[PUSH-DOWN] Tables in left subtree: " + leftTables);
            //System.out.println("[PUSH-DOWN] Tables in right subtree: " + rightTables);
            
            // Case 1: Predicate only references tables in left subtree
            if (leftTables.containsAll(predTables) && !rightTables.containsAll(predTables)) {
                //System.out.println("[PUSH-DOWN] ⬅️ Pushing predicate down to LEFT subtree");
                join.setLeft(pushPredicate(join.getLeft(), pred));
                return join;
            }
            
            // Case 2: Predicate only references tables in right subtree
            if (rightTables.containsAll(predTables) && !leftTables.containsAll(predTables)) {
                //System.out.println("[PUSH-DOWN] Pushing predicate down to RIGHT subtree");
                if (join.getRight() instanceof Operator.ScanOperator && predTables.size() == 1) {
                    //System.out.println("[PUSH-DOWN] Created select operator directly above right scan");

                    // If right is a scan and predicate is for a single table, apply directly
                    join.setRight(new Operator.SelectOperator(join.getRight(), pred));
                } else {
                    // For more complex right sides, recursively push down
                    join.setRight(pushPredicate(join.getRight(), pred));
                }
                return join;
            }
            
            // Case 3: Predicate references tables from both sides or neither
            // Can't push further down in this case
            //System.out.println("[PUSH-DOWN] Cannot push predicate further down (spans multiple subtrees)");

            return node;
        }
        //System.out.println("[PUSH-DOWN] Reached operator that cannot handle predicate pushdown: " + node.getClass().getSimpleName());

        return node;
    }
    
    /**
     * Recursively finds all tables referenced in an operator subtree.
     * 
     * @param op The operator to analyze
     * @return Set of table names used in this operator subtree
     */
    private static Set<String> findTablesInOperator(Operator op) {
        Set<String> tables = new HashSet<>();
        
        if (op instanceof Operator.ScanOperator) {
            // Get table name from scan operator
            Operator.ScanOperator scan = (Operator.ScanOperator) op;
            String tableName = scan.getTable(); 
            tables.add(tableName);
        }
        else if (op instanceof Operator.JoinOperator) {
            // For joins, combine tables from both children
            Operator.JoinOperator join = (Operator.JoinOperator) op;
            tables.addAll(findTablesInOperator(join.getLeft()));
            tables.addAll(findTablesInOperator(join.getRight()));
        }
        else if (op instanceof Operator.SelectOperator) {
            // For selects, get tables from child
            Operator.SelectOperator select = (Operator.SelectOperator) op;
            tables.addAll(findTablesInOperator(select.getChild())); 
        }
        
        return tables;
    }

    /**
     * Determines the output columns based on the SELECT clause.
     * Handles both explicit column lists and SELECT * syntax.
     * 
     * @param select The parsed SQL SELECT statement
     * @return List of column names to include in the query output
     * @throws JSQLParserException If there's an error accessing SQL expressions
     */
    private static List<String> getOutputColumns(PlainSelect select) throws JSQLParserException {
        if (select.getSelectItems().stream().anyMatch(item -> item.getExpression() instanceof AllColumns)) {
            //return getAllColumns(select);
        	 // For SELECT *, preserve the FROM clause table order
            List<String> tables = extractTables(select);
            List<String> columns = new ArrayList<>();
            
            // Add columns in the order of tables in FROM clause
            for (String table : tables) {
                List<String> tableColumns = Catalog.getInstance().getColumns(table);
                for (String col : tableColumns) {
                    columns.add(table + "." + col);
                }
            }
            
            return columns;
        }

        // Process explicit columns
        return select.getSelectItems().stream()
            .map(SelectItem::getExpression) // Extract the expression
            .map(Expression::toString) // Convert to column name
            .collect(Collectors.toList());
    }
    
    /**
     * Extracts the list of columns specified in the GROUP BY clause.
     * 
     * @param select The parsed SQL SELECT statement
     * @return List of fully qualified column names for grouping
     */
    private static List<String> extractGroupByColumns(PlainSelect select) {
    	ExpressionList<?> expressionList = select.getGroupBy().getGroupByExpressionList();
    	List<String> columns = expressionList.stream()
    	        .map(expr -> (Expression) expr)
    	        .map(expr -> {
    	            if (expr instanceof Column) {
    	                Column column = (Column) expr;
    	                String colName = column.getTable().getName() + "." + column.getColumnName();
    	                //System.out.println("[DEBUG] GroupBy Column: " + colName); 
    	                return colName;
    	            }
    	            return expr.toString();
    	        })
    	        .collect(Collectors.toList());
    	    //System.out.println("[DEBUG] Final GroupBy Columns: " + columns); 
    	    return columns;
        
    }
    
    /**
     * Extracts SUM aggregate expressions from the SELECT clause.
     * 
     * @param select The parsed SQL SELECT statement
     * @return List of expressions inside SUM() functions
     */
    private static List<Expression> extractSumExpressions(PlainSelect select) {
        //return 
        
        List<Expression> sums = select.getSelectItems().stream()
                .filter(item -> item.getExpression() instanceof Function)
                .map(item -> (Function) item.getExpression())
                .filter(func -> "SUM".equalsIgnoreCase(func.getName()))
                .map(func -> (Expression) func.getParameters().get(0)) // Explicit cast
                .collect(Collectors.toList());
        //System.out.println("[DEBUG] SUM Expressions: " + sums); 
        return sums;
    }    

    /**
     * Executes a query plan by repeatedly calling getNextTuple() on the root operator
     * and writing results to the specified output file.
     * 
     * @param root The root operator of the query plan
     * @param outputFile Path to the file where results should be written
     * @throws IOException If there's an error writing to the output file
     */
    public static void execute(Operator root, String outputFile) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            //root.reset();
            Tuple tuple;
            while ((tuple = root.getNextTuple()) != null) {
                writer.write(tuple.toString());
                writer.newLine();
            }
        }
    }
}