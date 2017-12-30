# ccc-skills-2
cloud computing competition skills 2



## Exploded

<20000,10> EXPLODED : 2,7 => 5,8,9,10

> Validation assumption ERROR?
> can A,B->C + A,B->D => A,B=>C,D?
> answer should be yes?



## Execution Times

- (200/20) Bare: 7'12
- (200/20) PubAttrsCut: 3'59
- (200/20) ..+RevTreeCut: 3'46


## Overall Procedure

At Driver

1. Generate all possible public attribute sets

2. Generate search space tree (Storage Graph-like Adjacency List) (Positive Version)

   To Every Public Attribute Set (still at driver)

   1. Repartition according to public attribute set

   2. Broadcast the search space tree

      To Every Partition (at executor)

      1. Generate search space tree (Adjacency List too) (Reversed Version)
      2. Validate each fd from broadcasted tree & update the generated tree

   3. Aggregate (reversed) trees from partitions

   4. Update the (positive) tree in driver using aggregated tree

3. Generate final possible fds from (positive) tree




## Implementation Details Notes



### Generate Combinations

- Use the Dict-Order Generator



### Adjacency List

- The key in secondary map is [<...lhs...>,rhs] e.g. lhs = 1,3 rhs = 2 then key=[1,3,2].
- Aggregation depends on the specification above, or will cause repetition in secondary maps



### Validate FDs

- Use the equivalence counts method described in paper



### Equivalence Counts

- Only storages the counts, due to only needs the counts in paper.