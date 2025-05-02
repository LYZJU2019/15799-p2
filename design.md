# optdbg

## Overview

The space of tools for evaluating query optimizers and explaining the causes of mis-optimizations is limited, and there is no existing tool that provides a satisfying end-to-end solution. As a result, much of query optimization debugging is done in an ad-hoc fashion or with bespoke proprietary tools.

This project seeks to create an end-to-end tool for detecting and explaining misoptizations. It does so by combining the approaches of existing work for evaluating optimizers (e.g. TAQO/OptMark) with approaches for explaining query plan regressions like in AutoDI. 

## Scope

This tool will be built around the DataFusion physical plan representation, and is thus dependent on an optimizer's query plan representation. In the context of `optd`, this means the tool relies on the [ongoing work for converting `optd` plans to DataFusion plans](https://github.com/cmu-db/optd/blob/connor/e2e/optd-datafusion/src/df_conversion/from_optd.rs). We choose to base our implementation primarily around optd-original to sidestep this fact.

This tool also relies on access to the memo table of a query optimizer to sample the space of alternative query plans. In the context of `optd` this would mean hooking into the [`egest` module](https://github.com/cmu-db/optd/blob/connor/e2e/optd-core/src/optimizer/egest.rs) to export more than just the chosen plans for the query and all subplans. As DataFusion doesn't have clear support for optimizer hints nor an explicit Cascades-style optimizer, this tool can achieve a similar strategy by using the [`datafusion-dolomite` optimizer](https://github.com/datafusion-contrib/datafusion-dolomite) and extracting plans from the [`memo` structure](https://github.com/datafusion-contrib/datafusion-dolomite/blob/main/dolomite/src/cascades/memo.rs). We make many methods public inside the `memo.rs` and `tasks2.rs` files in the archived copy of optd.

Furthermore, this tool also needs access to estimated costs of plans and subplans. We extract these from the cost model and the memo table

## Architectural Design

At the highest level, the component takes a compatible query optimizer and a set of SQL queries for evalution. It then outputs a report on which queries were misoptimized and why / how (alongside higher level information on the optimizer's performance e.g. how efficient it is). 

![query](https://hackmd.io/_uploads/H1K77XfCyl.png)

There are three primary subcomponents: 
- The **sampler** takes the input query and attempts to sample the space of possible plans for the query. This is the only subcomponent that directly interacts with the query optimizer being evaluated: it runs the optimizer on the query, gets the outputted plan, then extracts additional possible subplans/plans from the optimizer's memo table to get alternative plans.

  Abstractly, the "type signature" of this component would be
```rust
fn sample(query: String) -> Vec<Plan>
```
- The **benchmarker** takes the set of possible plans and runs each one of them using the DataFusion query engine (with a simple data source like Apache Arrow), measuring execution time and actual cardinality. It also runs all subplans to get the true cardinalities of each intermediate plan node. Then  "notable" plans (e.g. ones where the estimated cost is very different than the runtime performance) and higher level metrics such as the optimizer's efficiency are passed on by this component.

    The benchmarker component is designed to evaluate the performance and estimation accuracy of the query optimizer based on the following three dimensions:

    | Dimension | Guiding Question |
    |-----------|------------------|
    | **Accuracy** | Does the optimizer rank query plans correctly based on estimated cost vs actual performance? |
    | **Quality** | Are the internal estimates of cost and cardinality close to reality? |
    | **Efficiency** | How expensive is the optimization process in terms of plan search space and cost estimation overhead? |

    For each candidate plan sampled by the optimizer, we execute it and collect the following data:

    | Statistic | How It's Collected |
    |-----------|--------------------|
    | `actual_runtime` | Measured manually using wall-clock timing during plan execution |
    | `estimated_cost` | Retrieved from optd's cost model output |
    | `estimated_cardinalities` | Extracted from optd (via cost expressions or memo groups) |
    | `actual_cardinalities` | Counted by running subplans and collecting output row counts |
    | `plan_structure` | Dumped from the `ExecutionPlan` returned by optd |
     - Accuracy
    **Kendall’s Tau**
    Rank correlation between estimated cost and actual runtime.
     $\tau = \dfrac{C - D}{\binom{n}{2}}$
    **Rank Gap**  
        Difference between estimated rank and actual rank.  
        $\left| \text{estimated rank} - \text{actual rank} \right|$
    - Quality
    **Log-Error (Cost)**  
    How far estimated cost deviates from actual runtime.  
    $\left| \log_2 \left( \dfrac{\text{estimated cost}}{\text{runtime}} \right) \right|$
    **Log-Error (Cardinality)**  
    How far estimated rows deviate from actual rows.  
    $\left| \log_2 \left( \dfrac{\text{estimated rows}}{\text{actual rows}} \right) \right|$
    **Cost-Runtime Correlation**  
    Pearson correlation between estimated cost and actual runtime.  
    $r = \text{Pearson}(\text{cost},\ \text{runtime})$
    - Efficiency
    **# Logical Plans**  
    Number of logical plans explored by the optimizer.
    **# Physical Plans**  
    Number of physical plans costed.
    **# Join Orders**  
    Number of distinct join orders considered.
    **# Join Implementations**  
    Number of physical join algorithms tried.
    
  The "type signature" of this component would be something like below. Here `Vec<usize>` is meant to be the cardinalities of each node - attaching data to each node of a `Box<dyn Execution::Plan>` directly isn't straightforward and so it's maintained separately. The `Vec<f32>` is meant to be optimizer metrics.
```rust
fn benchmark(plans: Vec<Plan>) -> (Vec<(Plan, Vec<usize>)>, Vec<f32>) 
```
- The **analyzer** takes the set of plans with actual runtimes and cardinalities and does static analysis to attempt to explain why any misoptimizations occurred. This alongside some synthesis of the raw metrics from the benchmarker are presented in a report to the user.

  It would have a "type signature" like the following:
```rust
fn analyze(
    // Plans alongside actual cardinalities of each plan node
    card_plans: Vec<(Plan, Vec<usize>, chrono::Duration)>,
    metrics: Vec<f32>,
) -> String
```

### Configuration
Primary configuration knobs include:
- Which optimizer backend to test
- How large of a sample should be collected
- Whether true cardinalities should be measured for every subplan
- Timeout to use for benchmarking
- Desired "extra" measurements/analyses (e.g. optimizer sensitivity)

## Design Rationale

This design aims to be easily maintainable and to have a degree of parallelism in the implementation effort (that is to say it's comprised of a few mostly-independent pieces). Another primary design goal was to try to maximize the applicability of this tool while still being integratable with `optd` and maintaining a reasonable scope. Finally, a central design goal was to have a complete pipeline that transforms a query to a report on query optimizer quality.

This design achieves all of these goals: it is comprised of three major subcomponents with little interdependence and clear interfaces (and thus it is easily developed in parallel). For the second goal, this design focuses on using the increasingly-standard Apache DataFusion library as the central format/interface to the DBMS and thus is easily integratable into optd and other platforms. Finally, this design is a single end-to-end pipeline rather than a collection of tools, providing the desired UX missing from existing work. This pipeline design also makes for conveniently testable components and straightforward end-to-end testing. 


### Alternatives Considered
Not much in the way of alternatives were discussed for the high level pipeline design, but the individual subcomponents had earlier designs. An original design goal was to be fully generic, and thus: 
- The tool was intended to be used with an entire DBMS, not just an optimizer (or at the very least, an intended optimizer-DBMS combo)
- The sampler was to use optimizer hints to sample query plans from a database 
- The benchmarker was to use the DBMS in question to evaluate the plan performance

This approach added unneeded complexity to the design of this tool and widened the scope too much. Thus, the pared-down approach of working through DataFusion was eventually settled on.

## Testing Plan
Rather than maintain a set of known mis-optimizations, this component will be primarily tested by deliberately introducing regressions into a local copy of optd and testing if the resulting misoptimizations are detected by the tool. Hand-designed git patches (or patch templates) are applied to forked copy of optd and the tool run on TPC-H queries to ensure that the intended bugs are detected. 

While this forms a strategy for whole-tool integration tests, each subcomponent will have its own integration test. These will input the expected result from the previous subcomponent for an example query from TPC-H and check that the output is correct. In the cases of the sampler and benchmarker, these will be slightly loose / "sanity check" tests as rigorous testing would likely require some involved mocking logic.

Regular unit testing will still be used for individual pieces of logic within subcomponents. The same issues apply to the sampling / benchmarking components in that they are less contain less straightforwardly testable logic, but some degree of mocking and 

## Trade-offs and Potential Problems

There are three primary tradeoffs:
1. **Platform-dependence vs. scope**: the current design requires the optimizer can convert plans to DataFusion's physical plan format and requires direct access to the memo table. This limits the applicability of the tool, but in practice having to support a wider set of optimizers would necessitate adding features to `optd` (e.g. support for other formats, support for optimizer hints) and possibly using Java for the implementation (to use something like JDBC).
2. **Sampling quality vs. scope**: Sampling the space of query plans by using alternatives found in the memo table is not very effective. At the same time, `optd` is in a particularly volatile state at the current moment and thus implementing optimizer hint support (alongside the fact that DataFusion and `datafusion-dolomite` have no such support) is out of scope.
3. **Benchmarking speed vs. scope**: Naively running every subplan results  in a much slower benchmarking phase than necessary. At the same time, implementing the [covering query](http://www.vldb.org/pvldb/vol2/vldb09-294.pdf) optimization used by Microsoft expands the scope outside of what is likely reasonable. This is a more granular tradeoff where simpler heuristics can help.


## Future Work

Future work includes
- fully implementing [optimized cardinality measurement](http://www.vldb.org/pvldb/vol2/vldb09-294.pdf) for the set of alternate plans. This would make the tool run faster and potentially become viable for more traditional "debugging" usecases. This would in theory be a somewhat straightforward task of implementing the algorithm as described in the paper but would add to the general complexity of the benchmarking component.
- adding optimizer hint support to `optd`. This would let our tool use optimizer hints as a more generic and less invasive way of sampling alternative query plans, but would also be a great deal of effort given the tumultous state of `optd` at the moment.
- adding a fuzzing subcomponent that would result in the tool not depending on the user having a proper test workload (and thus being more useful). The potential difficulty of this varies: plugging in an existing SQL fuzzer may not be very challenging, but integrating SQL fuzzing capabilities in a way designed to efficiently find misoptimizations is a less obvious problem.
- supporting a wider set of optimizers. This would improve the usefulness of the tool, but increase complexity. Wider support would likely necessitate implementing optimizer hints as well as expanding the benchmarking component to support multiple query engines (which on its own ought not to be that difficult).

