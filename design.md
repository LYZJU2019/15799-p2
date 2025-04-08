# optdbg

## Overview

The space of tools for evaluating query optimizers and explaining the causes of mis-optimizations is limited, and there is no existing tool that provides a satisfying end-to-end solution. As a result, much of query optimization debugging is done in an ad-hoc fashion or with bespoke proprietary tools.

This project seeks to create an end-to-end tool for detecting and explaining misoptizations. It does so by combining the approaches of existing work for evaluating optimizers (e.g. TAQO/OptMark) with approaches for explaining query plan regressions like in AutoDI. 

## Scope

This tool will be built around the DataFusion physical plan representation, and is thus dependent on an optimizer's query plan representation. In the context of `optd`, this means the tool relies on the [ongoing work for converting `optd` plans to DataFusion plans](https://github.com/cmu-db/optd/blob/connor/e2e/optd-datafusion/src/df_conversion/from_optd.rs). 

This tool also relies on access to the memo table of a query optimizer to sample the space of alternative query plans. In the context of `optd` this would mean hooking into the [`egest` module](https://github.com/cmu-db/optd/blob/connor/e2e/optd-core/src/optimizer/egest.rs) to export more than just the chosen plans for the query and all subplans. As DataFusion doesn't have clear support for optimizer hints nor an explicit Cascades-style optimizer, this tool can achieve a similar strategy by using the [`datafusion-dolomite` optimizer](https://github.com/datafusion-contrib/datafusion-dolomite) and extracting plans from the [`memo` structure](https://github.com/datafusion-contrib/datafusion-dolomite/blob/main/dolomite/src/cascades/memo.rs).

Furthermore, this tool also needs access to estimated costs of plans and subplans. This ought not to require specific modifications to the optimizer as DataFusion's `ExecutionPlan` trait has a [`statistics` method](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.statistics) which itself has a [`num_rows` field](https://docs.rs/datafusion/latest/datafusion/common/struct.Statistics.html#structfield.num_rows). The tool does however rely on `optd`'s DataFusion export feature to implement the `statistics` method and populate the `num_rows` field.

## Architectural Design

At the highest level, the component takes a compatible query optimizer and a set of SQL queries for evalution. It then outputs a report on which queries were misoptimized and why / how (alongside higher level information on the optimizer's performance e.g. how efficient it is). 

![query](https://hackmd.io/_uploads/H1K77XfCyl.png)

There are three primary subcomponents: 
- The **sampler** takes the input query and attempts to sample the space of possible plans for the query. This is the only subcomponent that directly interacts with the query optimizer being evaluated: it runs the optimizer on the query, gets the outputted plan, then extracts additional possible subplans/plans from the optimizer's memo table to get alternative plans.

  Abstractly, the "type signature" of this component would be
```rust
fn sample(query: String) -> Vec<Arc<dyn datafusion::ExecutionPlan>>
```
- The **benchmarker** takes the set of possible plans and runs each one of them using the DataFusion query engine (with a simple data source like Apache Arrow), measuring execution time and actual cardinality. It also runs all subplans to get the true cardinalities of each intermediate plan node. Then  "notable" plans (e.g. ones where the estimated cost is very different than the runtime performance) and higher level metrics such as the optimizer's efficiency are passed on by this component.

  The "type signature" of this component would be something like below. Here `Vec<usize>` is meant to be the cardinalities of each node - attaching data to each node of a `Box<dyn Execution::Plan>` directly isn't straightforward and so it's maintained separately. The `Vec<f32>` is meant to be optimizer metrics.
```rust
fn benchmark(plans: Vec<Arc<dyn datafusion::ExecutionPlan>>) -> 
  (Vec<(Arc<dyn datafusion::ExecutionPlan>, Vec<usize>)>, Vec<f32>) 
```
- The **analyzer** takes the set of notable plans and does static analysis to attempt to explain why a misoptimization occurred. This alongside some synthesis of the raw metrics from the benchmarker are presented in a report to the user.

  It would have a "type signature" like the following:
```rust
fn analyze(
    // Plans alongside actual cardinalities of each plan node
    card_plans: Vec<(Arc<dyn datafusion::ExecutionPlan>, Vec<usize>)>, 
    metrics: Vec<f32>,
) -> String
```

## Design Rationale
>Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

This design aims to be easily maintainable and to have a degree of parallelism in the implementation effort (that is to say it's comprised of a few mostly-independent pieces). Another primary design goal was to try to maximize the applicability of this tool while still being integratable with `optd` and maintaining a reasonable scope. Finally, a central design goal was to have a complete pipeline that transforms a query to a report on query optimizer quality.

This design achieves all of these goals: it is comprised of three major subcomponents with little interdependence and clear interfaces (and thus it is easily developed in parallel). For the second goal, this design focuses on using the increasingly-standard Apache DataFusion library as the central format/interface to the DBMS and thus is easily integratable into optd and other platforms. Finally, this design is a single end-to-end pipeline rather than a collection of tools, providing the desired UX missing from existing work.

### Alternatives Considered
Not much in the way of alternatives were discussed for the high level pipeline design, but the individual subcomponents had earlier designs. An original design goal was to be fully generic, and thus: 
- The tool was intended to be used with an entire DBMS, not just an optimizer (or at the very least, an intended optimizer-DBMS combo)
- The sampler was to use optimizer hints to sample query plans from a database 
- The benchmarker was to use the DBMS in question to evaluate the plan performance

This approach added unneeded complexity to the design of this tool and widened the scope too much. Thus, the pared-down approach of working through DataFusion was eventually settled on.

## Testing Plan
Rather than maintain a set of known mis-optimizations, this component will be primarily tested by deliberately introducing regressions into a local copy of Apache DataFusion and testing if the resulting misoptimizations are detected by the tool. Hand-designed git patches (or patch templates) will be applied to a pinned version of the DataFusion codebase and the tool run on sample queries to ensure that the intended bugs are detected. 

While this forms a strategy for whole-tool integration tests, each subcomponent will have its own integration test. In the cases of the sampler and benchmarker, these will be slightly loose / "sanity check" tests as rigorous testing would likely require some involved mocking logic.

Regular unit testing will still be used for individual pieces of logic within subcomponents.

## Trade-offs and Potential Problems
>Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).

There are three primary tradeoffs:
1. **Platform-dependence vs. scope**: the current design requires the optimizer can convert plans to DataFusion's physical plan format and requires direct access to the memo table. This limits the applicability of the tool, but in practice having to support a wider set of optimizers would necessitate adding features to `optd` (e.g. support for other formats, support for optimizer hints) and possibly using Java for the implementation (to use something like JDBC).
2. **Sampling quality vs. scope**: Sampling the space of query plans by using alternatives found in the memo table is not very effective. At the same time, `optd` is in a particularly volatile state at the current moment and thus implementing optimizer hint support (alongside the fact that DataFusion and `datafusion-dolomite` have no such support) is out of scope.
3. **Benchmarking speed vs. scope**: Naively running every subplan results  in a much slower benchmarking phase than necessary. At the same time, implementing the [covering query](http://www.vldb.org/pvldb/vol2/vldb09-294.pdf) optimization used by Microsoft expands the scope outside of what is likely reasonable. This is a more granular tradeoff where simpler heuristics can help.

## Future Work

Future work includes
- fully implementing [optimized cardinality measurement](http://www.vldb.org/pvldb/vol2/vldb09-294.pdf) for the set of alternate plans
- adding optimizer hint support to `optd`
- adding a fuzzing subcomponent that would result in the tool not depending on the user having a proper test workload 
- supporting a wider set of optimizers

