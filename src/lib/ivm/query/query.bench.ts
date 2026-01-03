import { bench, describe } from 'vitest';
import { initialPosts, initialReactions, initialUsers } from '../test-data.js';
import { defaultComparator, Memory } from '../sources/memory.ts';
import { Query } from './query.ts';
import { CountOperator } from '../stream-operators/aggregation/count-operator.ts';
import { View } from '../sinks/view.ts';
import { count } from './aggregation.ts';
import { topBy } from '../test-counters.js';
import { JoinOperator } from '../stream-operators/join-operator.ts';
import { ProjectOperator } from '../stream-operators/project-operator.ts';
import { LimitOperator } from '../stream-operators/limit-operator.ts';
import { SplitStreamOperator } from '../stream-operators/split-stream-operator.ts';
import { GroupByOperator } from '../stream-operators/group-by-operator.ts';
import { CountGroupByOperator } from '../stream-operators/aggregation/count-group-by-operator.ts';
const createDb = () => {
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

	return {
		users: usersSource,
		posts: postsSource,
		reactions: reactionsSource
	};
};
describe('query vs operators count(*)', () => {
	const db1 = createDb();
	const totalUsersQuery = Query.using(db1)
		.from('users')
		.select([count('*')]);
	bench('query', () => {
		totalUsersQuery.execute();
	});

	const db2 = createDb();
	const countConn = db2.users.connect();
	const ct = new CountOperator(countConn, { column: 'id' });
	const countView = new View(ct, defaultComparator('count'));
	bench('operators', () => {
		countView.materialize();
	});
});

describe('query vs operators simple join', () => {
	const db1 = createDb();
	const topCountQ = Query.using(db1)
		.from('reactions')
		.groupBy(['reactions.userId'])
		.select([count('*'), 'reactions.userId']);

	const topReactorsQ = Query.using(db1)
		.with({ topCount: topCountQ })
		.from('users')
		.join('topCount', 'users.id', 'topCount.reactions.userId')
		.select([
			'topCount.reactions.userId as userId',
			'users.name as userName',
			'topCount.count(*) as count'
		])
		.limit(5);
	bench('query', () => {
		topReactorsQ.execute();
	});

	const db2 = createDb();
	const { materialize } = topBy({
		topSource: db2.reactions,
		bySource: db2.users,
		orderBy: (a, b) => {
			const countDiff = (b.count || 0) - (a.count || 0);
			if (countDiff !== 0) return countDiff;
			// Tie-breaker: use userId for stable ordering
			return a.userId - b.userId;
		},
		groupByKeys: ['userId'],
		countKeyExtractor: (reaction) => reaction.userId,
		joinKeyExtractor: (user) => user.id,
		limit: 5,
		select: {
			userId: (row) => row[0].userId,
			userName: (row) => row[1].name,
			count: (row) => row[0].count
		},
		// in this case, this is the same as `orderBy` but
		// join works on tuples so we need to specify the first one
		joinOrderBy: (a, b) => {
			const countA = a[0].count || 0;
			const countB = b[0].count || 0;
			const countDiff = countB - countA;
			if (countDiff !== 0) return countDiff;
			// Add tie-breaker using userId for stable ordering
			return a[0].userId - b[0].userId;
		}
	});
	bench('operators', () => {
		materialize();
	});
});

describe('complicated cte queries', () => {
	const db1 = createDb();

	const feedQuery = Query.using(db1)
		.from('posts')
		.join('users', 'posts.userId', 'users.id')
		.select([
			'posts.id as id',
			'posts.userId as userId',
			'posts.content as content',
			'posts.createdAt as createdAt',
			'users.name as userName'
		])
		.limit(20)
		.orderBy('createdAt', 'desc');
	const reactionCounts = Query.using(db1)
		.from('reactions')
		.groupBy(['reactions.postId', 'reactions.emoji'])
		.select([count('*').as('count'), 'reactions.postId as postId', 'reactions.emoji as emoji']);

	const feedWithReactionsQuery = Query.using(db1)
		.with({ feedQuery, reactionCounts })
		.from('feedQuery')
		.join('reactionCounts', 'feedQuery.id', 'reactionCounts.postId')
		.select([
			'feedQuery.id as id',
			'feedQuery.userId as userId',
			'feedQuery.content as content',
			'feedQuery.createdAt as createdAt',
			'feedQuery.userName as userName',
			'reactionCounts.emoji as emoji',
			'reactionCounts.count as reactionCount'
		])
		.orderBy('createdAt', 'desc');
	bench('query', () => {
		feedWithReactionsQuery.execute();
	});
	const db2 = createDb();

	const feedPostsConn = db2.posts.connect();
	const usersForFeedConn = db2.users.connect();

	const feedJoin = new JoinOperator<Post, User>(
		feedPostsConn,
		usersForFeedConn,
		(post) => post.userId,
		(user) => user.id,
		(a, b) => {
			if (a[0].createdAt > b[0].createdAt) return -1;
			if (a[0].createdAt < b[0].createdAt) return 1;
			return 0;
		}
	);

	// Project to flatten
	const feedProject = new ProjectOperator(feedJoin, {
		columns: {
			id: (row) => row[0].id,
			userId: (row) => row[0].userId,
			content: (row) => row[0].content,
			createdAt: (row) => row[0].createdAt,
			userName: (row) => row[1].name
		}
	});

	// Limit feed to 20 posts
	type FeedPost = {
		id: number;
		userId: number;
		content: string;
		createdAt: number;
		userName: string;
	};
	const feedLimit = new LimitOperator<FeedPost>(feedProject, 20, (a, b) => {
		if (a.createdAt > b.createdAt) return -1;
		if (a.createdAt < b.createdAt) return 1;
		return a.id - b.id;
	});

	// // Split feedLimit into two branches - one for the view, one for the join
	const feedLimitSplit = new SplitStreamOperator(feedLimit);
	const feedLimitForView = feedLimitSplit.branch();
	const feedLimitForJoin = feedLimitSplit.branch();

	const feedView = new View(feedLimitForView, (a, b) => {
		if (a.createdAt > b.createdAt) return -1;
		if (a.createdAt < b.createdAt) return 1;
		return a.id - b.id;
	});

	// ===== REACTIONS PER POST (for feed display) =====
	const reactionsPerPostConn = db1.reactions.connect();

	const reactionsPerPostComparator = (a: Reaction, b: Reaction) => {
		if (a.postId !== b.postId) return a.postId - b.postId;
		if (a.emoji < b.emoji) return -1;
		if (a.emoji > b.emoji) return 1;
		// Add unique ID as tie-breaker
		return a.id - b.id;
	};
	const reactionsPerPostGroup = new GroupByOperator(
		reactionsPerPostConn,
		['postId', 'emoji'],
		reactionsPerPostComparator
	);
	const reactionsPerPostCount = new CountGroupByOperator(reactionsPerPostGroup);
	// Join feed with reactions
	const feedWithReactionsJoin = new JoinOperator(
		feedLimitForJoin,
		reactionsPerPostCount,
		(post) => post.id,
		(reaction) => reaction.postId,
		(a, b) => {
			// Sort by post createdAt (descending), then by emoji
			if (a[0].createdAt > b[0].createdAt) return -1;
			if (a[0].createdAt < b[0].createdAt) return 1;
			if (a[1].emoji < b[1].emoji) return -1;
			if (a[1].emoji > b[1].emoji) return 1;
			if (a[0].id < b[0].id) return -1;
			if (a[0].id > b[0].id) return 1;
			return 0;
		}
	);

	const feedWithReactionsProject = new ProjectOperator(feedWithReactionsJoin, {
		columns: {
			id: (row) => row[0].id,
			userId: (row) => row[0].userId,
			content: (row) => row[0].content,
			createdAt: (row) => row[0].createdAt,
			userName: (row) => row[0].userName,
			emoji: (row) => row[1].emoji,
			reactionCount: (row) => row[1].count
		}
	});

	const feedWithReactionsView = new View(feedWithReactionsProject, reactionsPerPostComparator);

	bench('operators', () => {
		feedWithReactionsView.materialize();
	});
});
