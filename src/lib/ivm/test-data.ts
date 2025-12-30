export interface User {
	id: number;
	name: string;
	createdAt: number;
}

export interface Post {
	id: number;
	userId: number;
	content: string;
	createdAt: number;
}

export interface Reaction {
	id: number;
	userId: number;
	postId: number;
	emoji: string;
	createdAt: number;
}

const now = Date.now();

export const initialUsers: User[] = [
	{ id: 1, name: 'Alice', createdAt: now - 86400000 * 7 },
	{ id: 2, name: 'Bob', createdAt: now - 86400000 * 6 },
	{ id: 3, name: 'Charlie', createdAt: now - 86400000 * 5 },
	{ id: 4, name: 'Diana', createdAt: now - 86400000 * 4 },
	{ id: 5, name: 'Eve', createdAt: now - 86400000 * 3 }
];

export const initialPosts: Post[] = [
	{
		id: 1,
		userId: 1,
		content: 'Just deployed the new feature! 🚀',
		createdAt: now - 3600000 * 2
	},
	{
		id: 2,
		userId: 2,
		content: 'Anyone else excited about this new IVM system?',
		createdAt: now - 3600000 * 4
	},
	{
		id: 3,
		userId: 3,
		content: 'Working on some cool reactive queries today',
		createdAt: now - 3600000 * 6
	},
	{
		id: 4,
		userId: 4,
		content: 'The incremental view maintenance is so fast!',
		createdAt: now - 3600000 * 8
	},
	{
		id: 5,
		userId: 5,
		content: 'Check out this demo - real-time updates everywhere',
		createdAt: now - 3600000 * 10
	},
	{
		id: 6,
		userId: 1,
		content: 'Love how the stats update instantly',
		createdAt: now - 3600000 * 12
	},
	{
		id: 7,
		userId: 2,
		content: 'This is the future of reactive programming',
		createdAt: now - 3600000 * 14
	},
	{
		id: 8,
		userId: 3,
		content: 'B+ trees make everything so efficient',
		createdAt: now - 3600000 * 16
	}
];

export const initialReactions: Reaction[] = [
	// Post 1 reactions
	{ id: 1, userId: 2, postId: 1, emoji: '😀', createdAt: now - 3600000 * 2 + 60000 },
	{ id: 2, userId: 3, postId: 1, emoji: '❤️', createdAt: now - 3600000 * 2 + 120000 },
	{ id: 3, userId: 4, postId: 1, emoji: '👍', createdAt: now - 3600000 * 2 + 180000 },
	{ id: 4, userId: 5, postId: 1, emoji: '🎉', createdAt: now - 3600000 * 2 + 240000 },
	{ id: 5, userId: 1, postId: 1, emoji: '😀', createdAt: now - 3600000 * 2 + 300000 },

	// Post 2 reactions
	{ id: 6, userId: 1, postId: 2, emoji: '❤️', createdAt: now - 3600000 * 4 + 60000 },
	{ id: 7, userId: 3, postId: 2, emoji: '👍', createdAt: now - 3600000 * 4 + 120000 },
	{ id: 8, userId: 4, postId: 2, emoji: '😀', createdAt: now - 3600000 * 4 + 180000 },

	// Post 3 reactions
	{ id: 9, userId: 2, postId: 3, emoji: '🎉', createdAt: now - 3600000 * 6 + 60000 },
	{ id: 10, userId: 4, postId: 3, emoji: '❤️', createdAt: now - 3600000 * 6 + 120000 },
	{ id: 11, userId: 5, postId: 3, emoji: '👍', createdAt: now - 3600000 * 6 + 180000 },

	// Post 4 reactions
	{ id: 12, userId: 1, postId: 4, emoji: '😀', createdAt: now - 3600000 * 8 + 60000 },
	{ id: 13, userId: 2, postId: 4, emoji: '❤️', createdAt: now - 3600000 * 8 + 120000 },
	{ id: 14, userId: 3, postId: 4, emoji: '🎉', createdAt: now - 3600000 * 8 + 180000 },
	{ id: 15, userId: 5, postId: 4, emoji: '👍', createdAt: now - 3600000 * 8 + 240000 }
];

export const emojis = ['😀', '❤️', '👍', '🎉', '🔥', '💯'];

export function getRandomEmoji(): string {
	return emojis[Math.floor(Math.random() * emojis.length)];
}

export function getRandomUserId(maxId: number): number {
	return Math.floor(Math.random() * maxId) + 1;
}

export function generateRandomPost(id: number, userId: number): Post {
	const contents = [
		'This is amazing!',
		'Just discovered something cool',
		'Working on an interesting problem',
		'Love this community',
		'Great discussion today',
		'Excited about the future',
		'Learning so much',
		'This changes everything'
	];
	return {
		id,
		userId,
		content: contents[Math.floor(Math.random() * contents.length)],
		createdAt: Date.now()
	};
}

export function generateRandomReaction(id: number, userId: number, postId: number): Reaction {
	return {
		id,
		userId,
		postId,
		emoji: getRandomEmoji(),
		createdAt: Date.now()
	};
}

export function generateRandomUser(id: number): User {
	const names = [
		'Alex',
		'Jordan',
		'Taylor',
		'Morgan',
		'Casey',
		'Riley',
		'Avery',
		'Quinn',
		'Sage',
		'River'
	];
	return {
		id,
		name: names[Math.floor(Math.random() * names.length)] + id,
		createdAt: Date.now()
	};
}
