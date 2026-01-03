# Stream Operators

1. they are sinks and sources
2. they can only have 1 sink (except for a fan-out operator which splits a stream)
3. there are some that can have multiple sources (`JOIN`) or after a fan-out operation we need to fan-in back to a single stream
4. a source (e.g. sources/memory.ts) can have multiple `sink`s though each connection should only have 1 TOOD need to enforce that

# TODO

- fan out operator for creating independent branches:  `OR` conditionals, `UNION`, multiple aggregations on same data (e.g. `SELECT COUNT(*) as total, SUM(amount) as sum, AVG(amount) as avg FROM orders)`
- fan in operator for combining two streams together, `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`