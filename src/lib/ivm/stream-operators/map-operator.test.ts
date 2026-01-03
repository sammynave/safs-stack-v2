import { describe, expect, it } from 'vitest';
import { MapOperator } from './map-operator.ts';
import { JoinOperator } from './join-operator.ts';
import { Memory } from '../sources/memory.ts';

function universalComparator<T>(a: T, b: T): number {
	const aStr = JSON.stringify(a);
	const bStr = JSON.stringify(b);
	return aStr.localeCompare(bStr);
}

describe('map-operator', () => {
	it('flattens a single join', () => {
		// Setup: users and posts tables
		const users = new Memory({
			initialData: [
				{ id: 1, name: 'Alice' },
				{ id: 2, name: 'Bob' }
			],
			pk: 'id',
			schema: {}
		});

		const posts = new Memory({
			initialData: [
				{ id: 101, userId: 1, content: 'Hello' },
				{ id: 102, userId: 2, content: 'World' }
			],
			pk: 'id',
			schema: {}
		});

		// Join users and posts
		const joinOp = new JoinOperator(
			users.connect(),
			posts.connect(),
			(user) => user.id,
			(post) => post.userId,
			universalComparator
		);

		// Flatten the join result
		const flattenOp = new MapOperator(joinOp, ([user, post]) => {
			const flattened = {};
			for (const [key, val] of Object.entries(user)) {
				flattened[`users.${key}`] = val;
			}
			for (const [key, val] of Object.entries(post)) {
				flattened[`posts.${key}`] = val;
			}
			return flattened;
		});

		// Pull and verify results
		const results = Array.from(flattenOp.pull()).map(([row]) => row);

		expect(results).toEqual([
			{
				'users.id': 1,
				'users.name': 'Alice',
				'posts.id': 101,
				'posts.userId': 1,
				'posts.content': 'Hello'
			},
			{
				'users.id': 2,
				'users.name': 'Bob',
				'posts.id': 102,
				'posts.userId': 2,
				'posts.content': 'World'
			}
		]);
	});

	it('flattens two joins (users -> posts -> reactions)', () => {
		// Setup: users, posts, and reactions tables
		const users = new Memory({
			initialData: [
				{ id: 1, name: 'Alice' },
				{ id: 2, name: 'Bob' }
			],
			pk: 'id',
			schema: {}
		});

		const posts = new Memory({
			initialData: [
				{ id: 101, userId: 1, content: 'Hello' },
				{ id: 102, userId: 2, content: 'World' }
			],
			pk: 'id',
			schema: {}
		});

		const reactions = new Memory({
			initialData: [
				{ id: 201, postId: 101, emoji: '👍' },
				{ id: 202, postId: 101, emoji: '❤️' },
				{ id: 203, postId: 102, emoji: '🎉' }
			],
			pk: 'id',
			schema: {}
		});

		// First join: users -> posts
		const firstJoin = new JoinOperator(
			users.connect(),
			posts.connect(),
			(user) => user.id,
			(post) => post.userId,
			universalComparator
		);

		// Flatten first join
		const firstFlatten = new MapOperator(firstJoin, ([user, post]) => {
			const flattened = {};
			for (const [key, val] of Object.entries(user)) {
				flattened[`users.${key}`] = val;
			}
			for (const [key, val] of Object.entries(post)) {
				flattened[`posts.${key}`] = val;
			}
			return flattened;
		});

		// Second join: (users + posts) -> reactions
		const secondJoin = new JoinOperator(
			firstFlatten,
			reactions.connect(),
			(row) => row['posts.id'], // Extract from flattened row
			(reaction) => reaction.postId,
			universalComparator
		);

		// Flatten second join
		const secondFlatten = new MapOperator(secondJoin, ([leftRow, reaction]) => {
			const flattened = { ...leftRow }; // leftRow is already flattened
			for (const [key, val] of Object.entries(reaction)) {
				flattened[`reactions.${key}`] = val;
			}
			return flattened;
		});

		// Pull and verify results
		const results = Array.from(secondFlatten.pull()).map(([row]) => row);

		expect(results).toEqual([
			{
				'users.id': 1,
				'users.name': 'Alice',
				'posts.id': 101,
				'posts.userId': 1,
				'posts.content': 'Hello',
				'reactions.id': 201,
				'reactions.postId': 101,
				'reactions.emoji': '👍'
			},
			{
				'users.id': 1,
				'users.name': 'Alice',
				'posts.id': 101,
				'posts.userId': 1,
				'posts.content': 'Hello',
				'reactions.id': 202,
				'reactions.postId': 101,
				'reactions.emoji': '❤️'
			},
			{
				'users.id': 2,
				'users.name': 'Bob',
				'posts.id': 102,
				'posts.userId': 2,
				'posts.content': 'World',
				'reactions.id': 203,
				'reactions.postId': 102,
				'reactions.emoji': '🎉'
			}
		]);
	});
});
