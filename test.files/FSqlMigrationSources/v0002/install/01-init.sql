CREATE TABLE [subscriber]
(
	[id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	[subscriber_uuid] UUID NOT NULL,
	[topic_id] INT REFERENCES [topic]([id]) NOT NULL,
	[date_unix_create_date] INTEGER NOT NULL,
	[date_unix_delete_date] INTEGER NULL,
	CONSTRAINT [uq__subscriber__subscriber_uuid] UNIQUE ([subscriber_uuid])
);
