type Column = {
	type: 'text' | 'integer' | 'boolean';
	default?: string | number | boolean;
};

export type Tables = Record<
	string,
	{
		name: string;
		columns: Record<string, Column>;
		primaryKey: string;
	}
>;

export type Event<EventPayloadSchema> = {
	name: string;
	schema: EventPayloadSchema;
	synced: boolean;
};
