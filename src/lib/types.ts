type Column = {
	type: 'text' | 'integer' | 'boolean';
	default?: string | number | boolean;
	primaryKey?: boolean;
};

export type Tables = Record<
	string,
	{
		name: string;
		columns: Record<string, Column>;
	}
>;

export type Event<EventPayloadSchema> = {
	name: string;
	schema: EventPayloadSchema;
	synced: boolean;
};
