# IVM

High level components

1. Sources (to be renamed ReactiveTables ?)
2. StreamOperators
3. Sinks

Low level compoents
1. ChangeSet
2. B+ Tree

Concepts to be discarded/integrated

1. Circuits (from the DBSP paper in /resources) - i don't *think* this needs to be abstracted. it should become part of the LimitOperator
2. BilinearChangeSetAlgebra - this was ported from forseti but isn't being used (yet). might be needed for aggregates
3. ChangeSetOperators - not sure if we need this but it isn't currently being used
