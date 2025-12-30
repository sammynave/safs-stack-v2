/*
 *
 *                   Source
 *                     |
 *             SplitStreamOperator (5 branches)
 *             /    /    |    \    \
 *         Count Sum  Avg  Max  Min
 *             \    \    |    /    /
 *           CombineOperator/MultiRowCombineOperator (merge all)
 *                     |
 *       { count: 5, sum: 100, avg: 20, max: 50, min: 10 }
 */
export type AggregationDescriptor = {
	type: 'aggregation';
	fn: 'count' | 'sum' | 'avg' | 'min' | 'max' | 'arrayAgg' | 'jsonAgg';
	column: string | string[] | Record<string, string>;
	as: (alias: string) => AggregationDescriptor & { alias: string };
};

export function count(column: string): AggregationDescriptor {
	return {
		type: 'aggregation',
		fn: 'count',
		column,
		as: function (alias: string) {
			return { ...this, alias };
		}
	};
}

export function arrayAgg(column: string): AggregationDescriptor {
	return {
		type: 'aggregation',
		fn: 'arrayAgg',
		column,
		as: function (alias: string) {
			return { ...this, alias };
		}
	};
}

export function jsonAgg(column: string | string[] | Record<string, string>): AggregationDescriptor {
	return {
		type: 'aggregation' as const,
		fn: 'jsonAgg' as const,
		column,
		as: function (alias: string) {
			return { ...this, alias };
		}
	};
}

export function sum(column: string): AggregationDescriptor {
	return {
		type: 'aggregation',
		fn: 'sum',
		column,

		as: function (alias: string) {
			return { ...this, alias };
		}
	};
}

export function avg(column: string): AggregationDescriptor {
	return {
		type: 'aggregation',
		fn: 'avg',
		column,

		as: function (alias: string) {
			return { ...this, alias };
		}
	};
}

export function min(column: string): AggregationDescriptor {
	return {
		type: 'aggregation',
		fn: 'min',
		column,

		as: function (alias: string) {
			return { ...this, alias };
		}
	};
}

export function max(column: string): AggregationDescriptor {
	return {
		type: 'aggregation',
		fn: 'max',
		column,

		as: function (alias: string) {
			return { ...this, alias };
		}
	};
}
