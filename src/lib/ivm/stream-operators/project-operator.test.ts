import { describe, expect, it } from 'vitest';
import { Memory } from '../sources/memory.ts';
import { ProjectOperator } from './project-operator.ts';
import { View } from '../sinks/view.ts';

describe('ProjectOperator', () => {
	it('should project subset of columns', () => {
		type Order = { id: number; userId: number; amount: number };
		type Projected = { userId: number; amount: number };

		const orders = new Memory<Order>({
			initialData: [
				{ id: 1, userId: 10, amount: 100 },
				{ id: 2, userId: 20, amount: 200 }
			],
			pk: 'id',
			schema: null
		});

		const conn = orders.connect();
		const project = new ProjectOperator<Order, Projected>(conn, {
			columns: {
				userId: (row) => row.userId,
				amount: (row) => row.amount
			}
		});

		const view = new View(project, (a, b) => a.userId - b.userId);
		const results = view.materialize();

		expect(results).toEqual([
			{ userId: 10, amount: 100 },
			{ userId: 20, amount: 200 }
		]);
		expect(results[0]).not.toHaveProperty('id'); // id was dropped
	});

	it('should compute derived columns', () => {
		type Order = { id: number; amount: number };
		type WithTax = { id: number; subtotal: number; tax: number; total: number };

		const orders = new Memory<Order>({
			initialData: [{ id: 1, amount: 100 }],
			pk: 'id',
			schema: null
		});

		const conn = orders.connect();
		const project = new ProjectOperator<Order, WithTax>(conn, {
			columns: {
				id: (row) => row.id,
				amount: (row) => row.amount,
				doubled: (row) => row.amount * 2
			}
		});

		const view = new View(project, (a, b) => a.id - b.id);
		const results = view.materialize();

		expect(results).toEqual([{ id: 1, amount: 100, doubled: 200 }]);
	});

	it('should handle incremental updates efficiently', () => {
		type Order = { id: number; amount: number };
		type Projected = { id: number; doubled: number };

		const orders = new Memory<Order>({
			initialData: [{ id: 1, amount: 100 }],
			pk: 'id',
			schema: null
		});

		const conn = orders.connect();
		const project = new ProjectOperator<Order, Projected>(conn, {
			columns: {
				id: (row) => row.id,
				doubled: (row) => row.amount * 2
			}
		});

		const view = new View(project, (a, b) => a.id - b.id);

		// Initial state
		expect(view.materialize()).toEqual([{ id: 1, doubled: 200 }]);

		// Add new row - should only transform the delta
		orders.add({ id: 2, amount: 50 });

		expect(view.materialize()).toEqual([
			{ id: 1, doubled: 200 },
			{ id: 2, doubled: 100 }
		]);
	});

	it('should add constant columns', () => {
		type Order = { id: number; amount: number };
		type WithStatus = { id: number; amount: number; status: string; version: number };

		const orders = new Memory<Order>({
			initialData: [{ id: 1, amount: 100 }],
			pk: 'id',
			schema: null
		});

		const conn = orders.connect();
		const project = new ProjectOperator<Order, WithStatus>(conn, {
			columns: {
				id: (row) => row.id,
				amount: (row) => row.amount,
				status: () => 'ACTIVE',
				version: () => 1
			}
		});

		const view = new View(project, (a, b) => a.id - b.id);
		const results = view.materialize();

		expect(results).toEqual([{ id: 1, amount: 100, status: 'ACTIVE', version: 1 }]);
	});
});
