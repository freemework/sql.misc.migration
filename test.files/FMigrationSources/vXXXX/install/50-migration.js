async function migration(executionContext, sqlConnection, log) {
	log.info(executionContext, __filename);

	const idData = await sqlConnection.statement(
		`SELECT [id] FROM [topic] WHERE [name] = ?`
	).executeScalar(executionContext, "migration.js");

	await sqlConnection.statement(
		`INSERT INTO [subscriber] ([subscriber_uuid], [topic_id], [date_unix_create_date]) VALUES ('1fbb7a8a-cace-4a80-a9de-77ff14e6762d', ?, ?)`
	).execute(executionContext, idData.asInteger, Math.trunc(Date.now() / 1000));
}
