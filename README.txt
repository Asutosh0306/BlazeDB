################################
####         BlazeDB        #### 
################################



1. Join Condition Extraction Strategy

I have extracted join conditions from the WHERE clause using a clear method. First, I have broken down complex WHERE clauses into individual predicates. I did this by recursively splitting nested AND expressions in the splitConjunctivePredicates() method in BlazeDB.java. This provided better control over where each condition was placed in the query plan.

Each predicate underwent analysis to determine which tables it referenced in the extractJoinCondition() method. Based on this analysis, predicates are classified into three categories:
	1.	Single-table predicates, which become selection operations applied directly above the appropriate scan operators
	2.	Multi-table predicates that connect tables in a join, which become join conditions
	3.	Cross-join predicates (applied as high in the tree as necessary). I have explained this more in section 2.1 below.

When constructing the left-deep join tree (following FROM clause order), each join receives only the conditions relevant to the tables it combines. This ensures efficient filtering at the earliest possible stage of query execution.The implementation specifically avoids computing expensive cross products by identifying join conditions that connect tables and evaluating these conditions during join processing rather than afterward.


2. Query Optimization Techniques

I have implemented several optimization techniques that worked together to minimize intermediate result sizes and improve query evaluation performance.

2.1. Predicate Pushdown
My query optimizer pushes selection predicates as far down the operator tree as possible using the pushDownPredicates() and pushPredicate() methods in BlazeDB.java. This ensures that the filtering takes place early, reducing the number of tuples flowing through the operator pipeline. For complex queries with multiple joins, the optimizer analyses predicates to determine which could be evaluated early. Single-table predicates were pushed down to just above the scan operators, while join conditions were integrated directly into join operators.

When a predicate references tables from both the left and right subtrees of a join, it cannot be pushed further down. In this case, it is applied at the current level in the tree, which is still better than applying it at the very top of the tree.This optimization is correct because it preserves the original query’s semantics while adjusting when filtering occurs. It significantly reduces intermediate result sizes, especially for selective predicates on large tables.

2.2. Projection Pushdown
I have implemented an advanced projection pushdown optimization at the scan level through the collectRequiredColumns() method in BlazeDB.java. The ScanOperator class in Operator.java reads only the required columns instead of reading all columns and projecting later.This approach avoids deserializing unused data when scanning wide tables. For queries using only a subset of columns, it significantly reduces memory usage.

Although this design gave the ScanOperator dual responsibility of both reading and projecting. The performance benefits justify the trade-off. This was especially important for this project, where reducing data movement is critical.



3. Additional Information
The expression evaluator uses integer values (0/1) instead of boolean values for condition evaluation. While this approach works correctly; I was not able to debug it in time for the boolean-based implementation.

Both the SortOperator and aggregation operators (SumOperator and DuplicateEliminationOperator) freeze their entire input in memory before producing any output. This approach would require modification to handle very large datasets in a production environment.
