INSERT INTO [topic] ([name], [description], [media_type], [topic_security], [publisher_security], [subscriber_security], [date_unix_create_date])
VALUES ('market1', 'Market currency', 's', 's', 'd', 'as', 1577635382);

{{#musketeers}}
INSERT INTO [topic] ( [name], [description], [media_type], [topic_security], [publisher_security], [subscriber_security], [date_unix_create_date])
VALUES ('market2', '{{ . }}', '{{ direction }}', 's', 'd', 'as', 1577635383);
{{/musketeers}}
