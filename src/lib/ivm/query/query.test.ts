import { describe, expect, it } from 'vitest';
import { Query } from './query.ts';
import { Memory } from '../sources/memory.ts';
import { count, sum, avg, arrayAgg, jsonAgg } from './aggregation.ts';
import { initialPosts, initialReactions, initialUsers } from '../test-data.js';

function createDefaultDb() {
	const usersSource = new Memory({
		initialData: [
			{ id: 1, name: 'Alice' },
			{ id: 2, name: 'Bob' },
			{ id: 3, name: 'Charlie' },
			{ id: 4, name: 'Charlie' }
		],
		pk: 'id',
		schema: {}
	});

	const postsSource = new Memory({
		initialData: [
			{ id: 1, userId: 2, content: 'content 1' },
			{ id: 2, userId: 3, content: 'content 2' },
			{ id: 3, userId: 3, content: 'content 3' }
		],
		pk: 'id',
		schema: {}
	});

	const reactionsSource = new Memory({
		initialData: [
			// Post 1 reactions
			{ id: 1, userId: 2, postId: 1, emoji: '😀' },
			{ id: 2, userId: 3, postId: 1, emoji: '❤️' },
			{ id: 5, userId: 1, postId: 1, emoji: '😀' },

			// Post 2 reactions
			{ id: 6, userId: 1, postId: 2, emoji: '❤️' },
			{ id: 7, userId: 3, postId: 2, emoji: '👍' },

			// Post 3 reactions
			{ id: 9, userId: 2, postId: 3, emoji: '🎉' }
		],
		pk: 'id',
		schema: {}
	});

	return {
		users: usersSource,
		posts: postsSource,
		reactions: reactionsSource
	};
}

/* TODO
 * `WHERE ... OR ...
 *
 *  add '*' operator or something else
 * `SELECT * ...`
 * `SELECT DISTINCT * ...`
 */
describe('query builder', () => {
	it('COUNT(*) includes nulls, COUNT(column) excludes nulls', () => {
		const database = createDefaultDb();

		// Add a user with null name
		database.users.add({ id: 5, name: null });

		const countStarQuery = Query.using(database)
			.from('users')
			.select([count('*')]);

		const countColumnQuery = Query.using(database)
			.from('users')
			.select([count('users.name')]);

		expect(countStarQuery.execute()).toStrictEqual([{ 'count(*)': 5 }]); // includes null
		expect(countColumnQuery.execute()).toStrictEqual([{ 'count(users.name)': 4 }]); // excludes null
	});

	it('SELECT users.name FROM users', () => {
		const database = createDefaultDb();

		const userNamesQuery = Query.using(database).from('users').select(['users.name']);
		const userNames = userNamesQuery.execute();

		expect(userNames).toStrictEqual([
			{ 'users.name': 'Alice' },
			{ 'users.name': 'Bob' },
			{ 'users.name': 'Charlie' },
			{ 'users.name': 'Charlie' }
		]);
	});

	it("SELECT users.name FROM users WHERE users.name === 'Charlie'", () => {
		const database = createDefaultDb();

		const onlyCharliesQuery = Query.using(database)
			.from('users')
			.select(['users.name'])
			.where('users.name', '=', 'Charlie');

		const charlies = onlyCharliesQuery.execute();

		expect(charlies).toStrictEqual([{ 'users.name': 'Charlie' }, { 'users.name': 'Charlie' }]);
	});

	it("SELECT count(*) FROM users WHERE users.name === 'Charlie'", () => {
		const database = createDefaultDb();

		const onlyCharliesQuery = Query.using(database)
			.from('users')
			.select([count('*')])
			.where('users.name', '=', 'Charlie');

		const charlies = onlyCharliesQuery.execute();

		expect(charlies).toStrictEqual([{ 'count(*)': 2 }]);
	});

	it("SELECT count(*), users.id FROM users WHERE users.name === 'Charlie'", () => {
		const database = createDefaultDb();

		const onlyCharliesQuery = Query.using(database)
			.from('users')
			.select([count('*').as('countOfUsersWithId'), 'users.id'])
			.where('users.name', '=', 'Charlie')
			.groupBy(['users.id']);

		const charlies = onlyCharliesQuery.execute();

		expect(charlies).toStrictEqual([
			{ countOfUsersWithId: 1, 'users.id': 3 },
			{ countOfUsersWithId: 1, 'users.id': 4 }
		]);
	});

	it(`SELECT users.id, users.name, posts.content
      FROM users
      INNER JOIN posts ON users.id = posts.userId`, () => {
		const database = createDefaultDb();

		const onlyCharliesQuery = Query.using(database)
			.from('users')
			.join('posts', 'users.id', 'posts.userId')
			.select(['users.id', 'users.name', 'posts.content']);

		const charlies = onlyCharliesQuery.execute();

		expect(charlies).toStrictEqual([
			{ 'users.id': 2, 'users.name': 'Bob', 'posts.content': 'content 1' },
			{ 'users.id': 3, 'users.name': 'Charlie', 'posts.content': 'content 2' },
			{ 'users.id': 3, 'users.name': 'Charlie', 'posts.content': 'content 3' }
		]);
	});

	it(`SELECT users.id, users.name, posts.content
      FROM users
      LEFT JOIN posts ON users.id = posts.userId`, () => {
		const database = createDefaultDb();

		const onlyCharliesQuery = Query.using(database)
			.from('users')
			.leftOuterJoin('posts', 'users.id', 'posts.userId')
			.select(['users.id', 'users.name', 'posts.content']);

		const charlies = onlyCharliesQuery.execute();

		expect(charlies).toStrictEqual([
			{ 'users.id': 1, 'users.name': 'Alice', 'posts.content': null },
			{ 'users.id': 2, 'users.name': 'Bob', 'posts.content': 'content 1' },
			{ 'users.id': 3, 'users.name': 'Charlie', 'posts.content': 'content 2' },
			{ 'users.id': 3, 'users.name': 'Charlie', 'posts.content': 'content 3' },
			{ 'users.id': 4, 'users.name': 'Charlie', 'posts.content': null }
		]);
	});

	it(`SELECT users.id, users.name, posts.content
      FROM users
      INNER JOIN posts ON users.id = posts.userId
      LIMIT 2`, () => {
		const database = createDefaultDb();

		const only1CharliesQuery = Query.using(database)
			.select(['users.id', 'users.name', 'posts.content'])
			.from('users')
			.join('posts', 'users.id', 'posts.userId')
			.where('users.name', '=', 'Charlie')
			.limit(1);

		const charlies = only1CharliesQuery.execute();

		expect(charlies).toStrictEqual([
			{ 'users.id': 3, 'users.name': 'Charlie', 'posts.content': 'content 2' }
		]);
	});

	it('SELECT count(*), sum(reactions.id), avg(reactions.id) FROM reactions', () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('reactions')
			.select([count('*'), sum('reactions.id'), avg('reactions.id')]);

		const result = aggregationsQuery.execute();

		// Reactions: ids are 1, 2, 5, 6, 7, 9
		// count: 6
		// sum: 1 + 2 + 5 + 6 + 7 + 9 = 30
		// avg: 30 / 6 = 5
		expect(result).toStrictEqual([
			{
				'count(*)': 6,
				'sum(reactions.id)': 30,
				'avg(reactions.id)': 5
			}
		]);
	});

	it('SELECT ARRAY_AGG(users.name) FROM users', () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('users')
			.select([arrayAgg('users.name')]);

		let result = aggregationsQuery.execute();
		aggregationsQuery.subscribe((r) => (result = r));

		expect(result).toStrictEqual([
			{
				'arrayAgg(users.name)': ['Alice', 'Bob', 'Charlie', 'Charlie']
			}
		]);
		database.users.add({ id: 5, name: 'Barf' });
		expect(result).toStrictEqual([
			{
				'arrayAgg(users.name)': ['Alice', 'Bob', 'Charlie', 'Charlie', 'Barf']
			}
		]);
		database.users.remove({ id: 3, name: 'Charlie' });
		expect(result).toStrictEqual([
			{
				'arrayAgg(users.name)': ['Alice', 'Bob', 'Charlie', 'Barf']
			}
		]);
	});
	it('SELECT ARRAY_AGG(posts.content), users.id FROM posts JOIN users on users.id = posts.userId', () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('users')
			.join('posts', 'posts.userId', 'users.id')
			.select([arrayAgg('posts.content'), 'users.id'])
			.groupBy(['users.id']);

		let result = aggregationsQuery.execute();
		aggregationsQuery.subscribe((r) => (result = r));

		expect(result).toStrictEqual([
			{
				'arrayAgg(posts.content)': ['content 1'],
				'users.id': 2
			},
			{
				'arrayAgg(posts.content)': ['content 2', 'content 3'],
				'users.id': 3
			}
		]);
		database.posts.add({ id: 4, userId: 3, content: 'content 4' });
		expect(result).toStrictEqual([
			{
				'arrayAgg(posts.content)': ['content 1'],
				'users.id': 2
			},
			{
				'arrayAgg(posts.content)': ['content 2', 'content 3', 'content 4'],
				'users.id': 3
			}
		]);
		database.posts.remove({ id: 4, userId: 3, content: 'content 4' });
		expect(result).toStrictEqual([
			{
				'arrayAgg(posts.content)': ['content 1'],
				'users.id': 2
			},
			{
				'arrayAgg(posts.content)': ['content 2', 'content 3'],
				'users.id': 3
			}
		]);
		database.posts.remove({ id: 3, userId: 3, content: 'content 3' });
		database.posts.remove({ id: 2, userId: 3, content: 'content 2' });
		expect(result).toStrictEqual([
			{
				'arrayAgg(posts.content)': ['content 1'],
				'users.id': 2
			}
		]);
	});

	it('SELECT JSON_AGG(users.id) FROM users', () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('users')
			.select([jsonAgg('users.id')]);

		let result = aggregationsQuery.execute();
		aggregationsQuery.subscribe((r) => (result = r));

		expect(result).toStrictEqual([
			{
				'jsonAgg(users.id)': [1, 2, 3, 4]
			}
		]);

		database.users.add({ id: 5, name: 'Barf' });
		expect(result).toStrictEqual([
			{
				'jsonAgg(users.id)': [1, 2, 3, 4, 5]
			}
		]);

		database.users.remove({ id: 3, name: 'Charlie' });
		expect(result).toStrictEqual([
			{
				'jsonAgg(users.id)': [1, 2, 4, 5]
			}
		]);
	});

	it('SELECT JSON_AGG(users.name) FROM users', () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('users')
			.select([jsonAgg('users.name')]);

		let result = aggregationsQuery.execute();
		aggregationsQuery.subscribe((r) => (result = r));

		expect(result).toStrictEqual([
			{
				'jsonAgg(users.name)': ['Alice', 'Bob', 'Charlie', 'Charlie']
			}
		]);

		database.users.add({ id: 5, name: 'Barf' });
		expect(result).toStrictEqual([
			{
				'jsonAgg(users.name)': ['Alice', 'Bob', 'Charlie', 'Charlie', 'Barf']
			}
		]);

		database.users.remove({ id: 3, name: 'Charlie' });
		expect(result).toStrictEqual([
			{
				'jsonAgg(users.name)': ['Alice', 'Bob', 'Charlie', 'Barf']
			}
		]);
	});

	it('SELECT JSON_AGG(posts.id), users.id FROM posts JOIN users on users.id = posts.userId', () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('users')
			.join('posts', 'posts.userId', 'users.id')
			.select([jsonAgg('posts.id'), 'users.id'])
			.groupBy(['users.id']);

		let result = aggregationsQuery.execute();
		aggregationsQuery.subscribe((r) => (result = r));

		expect(result).toStrictEqual([
			{
				'jsonAgg(posts.id)': [1],
				'users.id': 2
			},
			{
				'jsonAgg(posts.id)': [2, 3],
				'users.id': 3
			}
		]);

		database.posts.add({ id: 4, userId: 3, content: 'content 4' });
		expect(result).toStrictEqual([
			{
				'jsonAgg(posts.id)': [1],
				'users.id': 2
			},
			{
				'jsonAgg(posts.id)': [2, 3, 4],
				'users.id': 3
			}
		]);

		database.posts.remove({ id: 4, userId: 3, content: 'content 4' });
		expect(result).toStrictEqual([
			{
				'jsonAgg(posts.id)': [1],
				'users.id': 2
			},
			{
				'jsonAgg(posts.id)': [2, 3],
				'users.id': 3
			}
		]);

		database.posts.remove({ id: 3, userId: 3, content: 'content 3' });
		database.posts.remove({ id: 2, userId: 3, content: 'content 2' });
		expect(result).toStrictEqual([
			{
				'jsonAgg(posts.id)': [1],
				'users.id': 2
			}
		]);
	});

	it(`SELECT
				reactions.postId,
				count(*),
				sum(reactions.id),
				avg(reactions.id)
			FROM reactions
			GROUP BY reactions.postId
		`, () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('reactions')
			.select(['reactions.postId', count('*'), sum('reactions.id'), avg('reactions.id')])
			.groupBy(['reactions.postId']);

		const result = aggregationsQuery.execute();

		// Post 1: reactions with ids 1, 2, 5 -> count: 3, sum: 8, avg: 8/3 ≈ 2.67
		// Post 2: reactions with ids 6, 7 -> count: 2, sum: 13, avg: 6.5
		// Post 3: reactions with id 9 -> count: 1, sum: 9, avg: 9
		expect(result).toStrictEqual([
			{
				'reactions.postId': 1,
				'count(*)': 3,
				'sum(reactions.id)': 8,
				'avg(reactions.id)': 8 / 3
			},
			{
				'reactions.postId': 2,
				'count(*)': 2,
				'sum(reactions.id)': 13,
				'avg(reactions.id)': 6.5
			},
			{
				'reactions.postId': 3,
				'count(*)': 1,
				'sum(reactions.id)': 9,
				'avg(reactions.id)': 9
			}
		]);
	});
	it(`SELECT
				posts.id,
				posts.content,
				count(*),
				sum(reactions.id)
			FROM posts
			INNER JOIN reactions ON posts.id = reactions.postId
			GROUP BY posts.id, posts.content`, () => {
		const database = createDefaultDb();

		const aggregationsQuery = Query.using(database)
			.from('posts')
			.join('reactions', 'posts.id', 'reactions.postId')
			.select(['posts.id', 'posts.content', count('*'), sum('reactions.id')])
			.groupBy(['posts.id', 'posts.content']);

		database.reactions.add({ id: 10, userId: 4, postId: 1, emoji: '🔥' });

		let result = aggregationsQuery.execute();

		expect(result).toStrictEqual([
			{
				'posts.id': 1,
				'posts.content': 'content 1',
				'count(*)': 4,
				'sum(reactions.id)': 18
			},
			{
				'posts.id': 2,
				'posts.content': 'content 2',
				'count(*)': 2,
				'sum(reactions.id)': 13
			},
			{
				'posts.id': 3,
				'posts.content': 'content 3',
				'count(*)': 1,
				'sum(reactions.id)': 9
			}
		]);

		aggregationsQuery.subscribe((r) => {
			result = r;
		});

		database.reactions.add({ id: 11, userId: 4, postId: 1, emoji: '🔥' });

		expect(result).toStrictEqual([
			{
				'posts.id': 1,
				'posts.content': 'content 1',
				'count(*)': 5,
				'sum(reactions.id)': 29
			},
			{
				'posts.id': 2,
				'posts.content': 'content 2',
				'count(*)': 2,
				'sum(reactions.id)': 13
			},
			{
				'posts.id': 3,
				'posts.content': 'content 3',
				'count(*)': 1,
				'sum(reactions.id)': 9
			}
		]);
	});

	it(`SELECT users.name
			FROM users
			WHERE users.name = 'Charlie'
			AND users.id = 3
		`, () => {
		const database = createDefaultDb();

		const onlyCharliesQuery = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.name', '=', 'Charlie')
			.and('users.id', '=', 3);

		const charlies = onlyCharliesQuery.execute();

		expect(charlies).toStrictEqual([{ 'users.id': 3, 'users.name': 'Charlie' }]);
	});

	it('SELECT DISTINCT users.name FROM users', () => {
		const database = createDefaultDb();

		const distinctNamesQuery = Query.using(database)
			.from('users')
			.select(['users.name'])
			.distinct();

		const result = distinctNamesQuery.execute();

		// Should have 3 unique names (Alice, Bob, Charlie) instead of 4 rows
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice' },
			{ 'users.name': 'Bob' },
			{ 'users.name': 'Charlie' }
		]);
	});

	it('SELECT DISTINCT ON (users.name) users.name, users.id FROM users', () => {
		const database = createDefaultDb();

		const distinctOnQuery = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.distinctOn(['users.name']);

		const result = distinctOnQuery.execute();

		// Should have 3 rows - one per unique name
		// For Charlie (appears twice with id:3 and id:4), we get the first one (id:3)
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Charlie', 'users.id': 3 }
		]);
	});

	it("SELECT users.name FROM users WHERE users.name = 'Alice' OR users.name = 'Bob'", () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.name', '=', 'Alice')
			.or('users.name', '=', 'Bob');

		const result = query.execute();

		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 }
		]);
	});

	it("SELECT users.name FROM users WHERE users.id = 1 AND users.name = 'Bob' OR users.name = 'Charlie'", () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.id', '=', 1)
			.and('users.name', '=', 'Bob') // (id=1 AND name='Bob') = false
			.or('users.name', '=', 'Charlie'); // false OR name='Charlie' = true for Charlies

		const result = query.execute();

		// Should return both Charlies since (id=1 AND name='Bob') is false, OR name='Charlie' is true
		expect(result).toStrictEqual([
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 }
		]);
	});

	it('SELECT count(*) FROM users WHERE users.id = 1 OR users.id = 2 OR users.id = 3', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select([count('*')])
			.where('users.id', '=', 1)
			.or('users.id', '=', 2)
			.or('users.id', '=', 3);

		const result = query.execute();

		expect(result).toStrictEqual([{ 'count(*)': 3 }]);
	});
});

describe('WHERE with OR', () => {
	// Test data: Alice(1), Bob(2), Charlie(3), Charlie(4)

	it('Basic OR: a OR b', () => {
		const db = createDefaultDb();
		const result = Query.using(db)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.name', '=', 'Alice')
			.or('users.name', '=', 'Bob')
			.execute();

		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 }
		]);
	});

	it('AND precedence: a OR b AND c → a OR (b AND c)', () => {
		const db = createDefaultDb();
		const result = Query.using(db)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.id', '=', 1) // a
			.or('users.name', '=', 'Charlie') // OR b
			.and('users.id', '=', 3) // AND c
			.execute();

		// Should be: id=1 OR (name='Charlie' AND id=3)
		// Returns: Alice(1) and Charlie(3)
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Charlie', 'users.id': 3 }
		]);
	});

	it('Explicit grouping: a AND (b OR c)', () => {
		const db = createDefaultDb();
		const result = Query.using(db)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.name', '=', 'Charlie') // a
			.and(
				(q) =>
					q // AND (
						.where('users.id', '=', 3) //   b
						.or('users.id', '=', 4) //   OR c
			) // )
			.execute();

		// Should be: name='Charlie' AND (id=3 OR id=4)
		// Returns: both Charlies
		expect(result).toStrictEqual([
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 }
		]);
	});

	it('Complex nesting: (a OR b) AND (c OR d)', () => {
		const db = createDefaultDb();
		const result = Query.using(db)
			.from('users')
			.select(['users.name', 'users.id'])
			.where(
				(q) =>
					q // (
						.where('users.id', '=', 2) //   a
						.or('users.id', '=', 3) //   OR b
			) // )
			.and(
				(q) =>
					q // AND (
						.where('users.name', '=', 'Bob') //   c
						.or('users.name', '=', 'Charlie') //   OR d
			) // )
			.execute();

		// Should be: (id=2 OR id=3) AND (name='Bob' OR name='Charlie')
		// Returns: Bob(2) and Charlie(3)
		expect(result).toStrictEqual([
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Charlie', 'users.id': 3 }
		]);
	});

	it('Multiple ORs: a OR b OR c', () => {
		const db = createDefaultDb();
		const result = Query.using(db)
			.from('users')
			.select(['users.id'])
			.where('users.id', '=', 1)
			.or('users.id', '=', 2)
			.or('users.id', '=', 3)
			.execute();

		expect(result).toStrictEqual([{ 'users.id': 1 }, { 'users.id': 2 }, { 'users.id': 3 }]);
	});

	it('Chained AND with OR: a AND b OR c AND d → (a AND b) OR (c AND d)', () => {
		const db = createDefaultDb();
		const result = Query.using(db)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.name', '=', 'Alice') // a
			.and('users.id', '=', 1) // AND b
			.or('users.name', '=', 'Bob') // OR c
			.and('users.id', '=', 2) // AND d
			.execute();

		// Should be: (name='Alice' AND id=1) OR (name='Bob' AND id=2)
		// Returns: Alice(1) and Bob(2)
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 }
		]);
	});

	it('Nested groups with OR: (a AND (b OR c)) OR d', () => {
		const db = createDefaultDb();
		const result = Query.using(db)
			.from('users')
			.select(['users.name', 'users.id'])
			.where(
				(q) =>
					q // (
						.where('users.name', '=', 'Charlie') //   a
						.and(
							(q2) =>
								q2 //   AND (
									.where('users.id', '=', 3) //     b
									.or('users.id', '=', 4) //     OR c
						) //   )
			) // )
			.or('users.name', '=', 'Alice') // OR d
			.execute();

		// Should be: (name='Charlie' AND (id=3 OR id=4)) OR name='Alice'
		// Returns: Alice(1), Charlie(3), Charlie(4)
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 }
		]);
	});
});

describe('ORDER BY', () => {
	it('SELECT users.name FROM users ORDER BY users.name ASC', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.orderBy('users.name', 'asc');

		const result = query.execute();

		// Should be sorted alphabetically: Alice, Bob, Charlie, Charlie
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 }
		]);
	});

	it('SELECT users.name FROM users ORDER BY users.name DESC', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.orderBy('users.name', 'desc');

		const result = query.execute();

		// Should be sorted reverse alphabetically: Charlie, Charlie, Bob, Alice
		expect(result).toStrictEqual([
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 },
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Alice', 'users.id': 1 }
		]);
	});

	it('SELECT users.name FROM users ORDER BY users.name ASC, users.id DESC (multi-column)', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.orderBy('users.name', 'asc')
			.orderBy('users.id', 'desc'); // Secondary sort for ties

		const result = query.execute();

		// Alice (id:1), Bob (id:2), then Charlies sorted by id DESC (4, then 3)
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Charlie', 'users.id': 4 }, // id DESC
			{ 'users.name': 'Charlie', 'users.id': 3 }
		]);
	});

	it('SELECT posts.content FROM posts ORDER BY posts.id DESC', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('posts')
			.select(['posts.id', 'posts.content'])
			.orderBy('posts.id', 'desc');

		const result = query.execute();

		expect(result).toStrictEqual([
			{ 'posts.id': 3, 'posts.content': 'content 3' },
			{ 'posts.id': 2, 'posts.content': 'content 2' },
			{ 'posts.id': 1, 'posts.content': 'content 1' }
		]);
	});

	it('ORDER BY with joined columns - ORDER BY posts.content', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.join('posts', 'users.id', 'posts.userId')
			.select(['users.name', 'posts.content'])
			.orderBy('posts.content', 'desc'); // Sort by joined column

		const result = query.execute();

		// Should be sorted by posts.content DESC: content 3, content 2, content 1
		expect(result).toStrictEqual([
			{ 'users.name': 'Charlie', 'posts.content': 'content 3' },
			{ 'users.name': 'Charlie', 'posts.content': 'content 2' },
			{ 'users.name': 'Bob', 'posts.content': 'content 1' }
		]);
	});

	it('ORDER BY with WHERE - filters then sorts', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.where('users.name', '=', 'Charlie')
			.orderBy('users.id', 'desc');

		const result = query.execute();

		// Only Charlies, sorted by id DESC
		expect(result).toStrictEqual([
			{ 'users.name': 'Charlie', 'users.id': 4 },
			{ 'users.name': 'Charlie', 'users.id': 3 }
		]);
	});

	it('ORDER BY with LIMIT - top K in order', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.orderBy('users.name', 'asc')
			.limit(2);

		const result = query.execute();

		// First 2 when sorted by name: Alice, Bob
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 }
		]);
	});

	it('ORDER BY with GROUP BY and aggregations', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('reactions')
			.select(['reactions.postId', count('*'), sum('reactions.id')])
			.groupBy(['reactions.postId'])
			.orderBy('count(*)', 'desc'); // Sort by aggregation result

		const result = query.execute();

		// Post 1 has 3 reactions, Post 2 has 2, Post 3 has 1
		expect(result).toStrictEqual([
			{
				'reactions.postId': 1,
				'count(*)': 3,
				'sum(reactions.id)': 8
			},
			{
				'reactions.postId': 2,
				'count(*)': 2,
				'sum(reactions.id)': 13
			},
			{
				'reactions.postId': 3,
				'count(*)': 1,
				'sum(reactions.id)': 9
			}
		]);
	});

	it('ORDER BY maintains order with incremental updates', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name', 'users.id'])
			.orderBy('users.name', 'asc');

		let result = query.execute();
		query.subscribe((r) => {
			result = r;
		});

		// Initial state
		expect(result).toStrictEqual([
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 }
		]);

		// Add a user that should appear first
		database.users.add({ id: 5, name: 'Aaron' });

		expect(result).toStrictEqual([
			{ 'users.name': 'Aaron', 'users.id': 5 }, // New first
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 }
		]);

		// Add a user that should appear last
		database.users.add({ id: 6, name: 'Zoe' });

		expect(result).toStrictEqual([
			{ 'users.name': 'Aaron', 'users.id': 5 },
			{ 'users.name': 'Alice', 'users.id': 1 },
			{ 'users.name': 'Bob', 'users.id': 2 },
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 },
			{ 'users.name': 'Zoe', 'users.id': 6 } // New last
		]);

		// Remove a user
		database.users.remove({ id: 2, name: 'Bob' });

		expect(result).toStrictEqual([
			{ 'users.name': 'Aaron', 'users.id': 5 },
			{ 'users.name': 'Alice', 'users.id': 1 },
			// Bob removed
			{ 'users.name': 'Charlie', 'users.id': 3 },
			{ 'users.name': 'Charlie', 'users.id': 4 },
			{ 'users.name': 'Zoe', 'users.id': 6 }
		]);
	});

	it('ORDER BY with DISTINCT - sorts unique values', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name'])
			.distinct()
			.orderBy('users.name', 'desc');

		const result = query.execute();

		// Unique names sorted DESC: Charlie, Bob, Alice
		expect(result).toStrictEqual([
			{ 'users.name': 'Charlie' },
			{ 'users.name': 'Bob' },
			{ 'users.name': 'Alice' }
		]);
	});

	it('ORDER BY with complex join and multi-column sort', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.join('posts', 'users.id', 'posts.userId')
			.select(['users.name', 'posts.id', 'posts.content'])
			.orderBy('users.name', 'asc')
			.orderBy('posts.id', 'desc'); // Secondary sort

		const result = query.execute();

		// Bob (post 1), then Charlie (posts 3, 2 in DESC order)
		expect(result).toStrictEqual([
			{ 'users.name': 'Bob', 'posts.id': 1, 'posts.content': 'content 1' },
			{ 'users.name': 'Charlie', 'posts.id': 3, 'posts.content': 'content 3' },
			{ 'users.name': 'Charlie', 'posts.id': 2, 'posts.content': 'content 2' }
		]);
	});
});

describe('SELECT alias', () => {
	it('SELECT as', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name as name', 'users.id as id'])
			.orderBy('users.name', 'asc');

		const result = query.execute();

		// Should be sorted alphabetically: Alice, Bob, Charlie, Charlie
		expect(result).toStrictEqual([
			{ name: 'Alice', id: 1 },
			{ name: 'Bob', id: 2 },
			{ name: 'Charlie', id: 3 },
			{ name: 'Charlie', id: 4 }
		]);
	});
	it('SELECT AS', () => {
		const database = createDefaultDb();

		const query = Query.using(database)
			.from('users')
			.select(['users.name AS name', 'users.id AS id'])
			.orderBy('users.name', 'asc');

		const result = query.execute();

		// Should be sorted alphabetically: Alice, Bob, Charlie, Charlie
		expect(result).toStrictEqual([
			{ name: 'Alice', id: 1 },
			{ name: 'Bob', id: 2 },
			{ name: 'Charlie', id: 3 },
			{ name: 'Charlie', id: 4 }
		]);
	});
});

describe('converting topBy to query', () => {
	it('should match old topBy results', () => {
		const usersSource = new Memory({
			initialData: initialUsers,
			pk: 'id',
			schema: {}
		});

		const postsSource = new Memory({
			initialData: initialPosts,
			pk: 'id',
			schema: {}
		});

		const reactionsSource = new Memory({
			initialData: initialReactions,
			pk: 'id',
			schema: {}
		});
		const db = { users: usersSource, posts: postsSource, reactions: reactionsSource };

		const topCount = Query.using(db)
			.from('reactions')
			.groupBy(['reactions.userId'])
			.select([count('*'), 'reactions.userId']);

		const query = Query.using(db)
			.with({ topCount })
			.from('users')
			.join('topCount', 'users.id', 'topCount.reactions.userId')
			.select([
				'topCount.reactions.userId as userId',
				'users.name as userName',
				'topCount.count(*) as count'
			])
			.limit(5);

		let topReactorsN = query.execute();
		query.subscribe((r) => {
			topReactorsN = r;
		});
		expect([
			{
				userId: 1,
				userName: 'Alice',
				count: 3
			},
			{
				userId: 2,
				userName: 'Bob',
				count: 3
			},
			{
				userId: 3,
				userName: 'Charlie',
				count: 3
			},
			{
				userId: 4,
				userName: 'Diana',
				count: 3
			},
			{
				userId: 5,
				userName: 'Eve',
				count: 3
			}
		]).toStrictEqual(topReactorsN);
	});
});
