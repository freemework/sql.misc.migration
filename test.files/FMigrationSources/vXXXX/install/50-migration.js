async function migration(executionContext, sqlProvider, log) {
	log.info(__filename);

	const idData = await sqlProvider.statement(
		`SELECT [id] FROM [topic] WHERE [name] = ?`
	).executeScalar(executionContext, "migration.js");

	await sqlProvider.statement(
		`INSERT INTO [subscriber] ([subscriber_uuid], [topic_id], [date_unix_create_date]) VALUES ('1fbb7a8a-cace-4a80-a9de-77ff14e6762d', ?, ?)`
	).execute(executionContext, idData.asInteger, Math.trunc(Date.now() / 1000));
}
