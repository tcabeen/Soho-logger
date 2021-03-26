# Router log ETL script

Run this script to ingest router logs into PostgreSQL database.
Note: Expects a format that matches the Peplink Soho router.

## Arguments

--log_directory | -d
Default: /var/log/soho/
Provide a fully qualified source directory

--match_regex | -m
Default: 'soho.log-\d{8}'
Provide a regular expression to limit log ingestion if desired

## Logging
Nah, it just prints status to STDOUT.

Cheers.
