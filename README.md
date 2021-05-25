# Volunteer Augmentation of ArchivesSpace Records

We are using nightly backups of the ArchivesSpace database to track changes made to the repository.

A `settings.ini` file is used to configure the usernames to track the changes of, the location of the local Git repository, and the location of the mysqldump files.

We are only tracking changes in specific tables of the database:

- `deleted_records`
- `agent_corporate_entity`
- `agent_person`
- `archival_object`
- `resource`
- `subject`

The script works in two modes: with and without an `--init` flag.

## With `--init` flag

We save the `deleted_records` table as a CSV file in the changes repository.

For the other tables we export a full ArchivesSpace record in JSON format of every item in each of the database tables via the API. We remove a number of superfluous elements from the JSON as well as identifying information for archivists.

These baseline JSON records are saved on the filesystem and must manually be added and committed to a Git repository.

## Without `--init` flag

Running without the `--init` flag is normal use for this script, but the baseline files must exist for expected results.

First we verify that a repository exists in a working state.

Next we create temporary CSV versions of the database tables from the last backup file and the penultimate backup file. We then compare these two tables and extract the identifier columns from any row that has been added, modified, or deleted. (We ignore superfluous columns in the tables.)

For each added or modified record we save a new JSON export of the ArchiveSpace data, overwriting existing files. If the record was added or modified by a volunteer username listed in the `settings.ini` file we include the identifying information in the JSON data.

Finally we separately add and commit the archivist-added or -modified files, the deleted files (and `deleted_records.csv` file), and the volunteer-added or -modified files.

This creates up to three separate commits for each date that contains changes to the ArchivesSpace database. The volunteer changes will be noted by a commit labeled "👀 volunteer changes [archivesspace-YYYY-MM-DD]"

## TODO

- Specifics of the database schema are used to identify superfluous columns in our source tables and these values are subject to change in an upgrade of the ArchivesSpace application. We need to keep this script up to date with the running version of the ArchivesSpace schema and can use the `version` value in the `schema_info` table of the database to be sure this script will continue to work.
