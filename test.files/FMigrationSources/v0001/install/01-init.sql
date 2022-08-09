CREATE TABLE [topic] (
	[id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	[name] VARCHAR(256) NOT NULL,
	[description] VARCHAR(1028) NOT NULL,
	[media_type] VARCHAR(1028) NOT NULL,
	[topic_security] VARCHAR(1028) NOT NULL,
	[publisher_security] VARCHAR(1028) NOT NULL,
	[subscriber_security] VARCHAR(1028) NOT NULL,
	[date_unix_create_date] INTEGER NOT NULL,
	[date_unix_delete_date] INTEGER NULL,
	CONSTRAINT [uq_topic_name] UNIQUE ([name])
)

-- GO
CREATE TABLE [topic2] (
	[id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	[name] VARCHAR(256) NOT NULL,
	[description] VARCHAR(1028) NOT NULL,
	[media_type] VARCHAR(1028) NOT NULL,
	[topic_security] VARCHAR(1028) NOT NULL,
	[publisher_security] VARCHAR(1028) NOT NULL,
	[subscriber_security] VARCHAR(1028) NOT NULL,
	[date_unix_create_date] INTEGER NOT NULL,
	[date_unix_delete_date] INTEGER NULL,
	CONSTRAINT [uq_topic_name] UNIQUE ([name])
);
