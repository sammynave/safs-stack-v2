# Known avenues for performance improvments

1. using stream operators directly gives you the ability to pass in targeted comparators. we lose that with the the DX we want from `query.ts`. using primary key as a comparator rather than comparing all columns could help. maybe there's some other trick or technique out there
2. we're using `MapOperator` for convinence but it adds ~5 extra loops/operators in the stream. keeping the data as tuples or unflattened through out the pipe line might be annoying but it should make things a lot faster. look at these Simulate Activity resuls from demo and demo-old routes
   1. raw operators: simulate activity: 226714 ms
   2. query builder: simulate activity: 393071 ms
3. an actual query planner would be nice.
4. maybe adding some ability hand tune (similar to 1) queries could be useful
