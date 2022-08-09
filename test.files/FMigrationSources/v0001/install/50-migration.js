async function migration(executionContext, sqlProvider, log) {
	log.info(__filename);
	await sqlProvider.statement(
		`INSERT INTO [topic] ([name], [description], [media_type], [topic_security], [publisher_security], [subscriber_security], [date_unix_create_date]) VALUES (?, ?, ?, ?, ?, ?, ?)`
	).execute(executionContext, 'migration.js', 'Market currency', 's', 's', 'd', 'as', Math.trunc(Date.now() / 1000));
}
