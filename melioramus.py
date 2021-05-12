import csv
from datetime import datetime
import difflib
import gzip
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

from asnake.client import ASnakeClient
from decouple import config
from decouple import Csv
import git
import pandas as pd

asnake_client = ASnakeClient()
asnake_client.authorize()

volunteers = config("VOLUNTEERS", cast=Csv())
output_dir = config("GIT_REPOSITORY")

# get files sorted by last modified
def get_files_last_modified_desc(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return sorted(paths, key=os.path.getmtime, reverse=True)


def get_table_name(line):
    start = line.find("INSERT INTO `") + len("INSERT INTO `")
    end = line.find("` VALUES")
    table = line[start:end]
    return table


def make_safe_filename(s):
    def safe_char(c):
        if c.isalnum():
            return c
        else:
            return "_"

    return "".join(safe_char(c) for c in s).rstrip("_")


def values_sanity_check(values):
    """
    Ensures that values from the INSERT statement meet basic checks.
    """
    assert values
    assert values[0] == "("
    # Assertions have not been raised
    return True


def parse_values(values, outfile):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """
    latest_row = []

    reader = csv.reader(
        [values],
        delimiter=",",
        doublequote=False,
        escapechar="\\",
        quotechar="'",
        strict=True,
    )

    with open(outfile, "a") as csvfile:
        writer = csv.writer(csvfile)
        for reader_row in reader:
            for column in reader_row:
                # If our current string is empty...
                if len(column) == 0 or column == "NULL":
                    # by inserting the string "NULL" we ensure the file is not
                    # interpreted as a binary file by `diff`
                    latest_row.append("NULL")
                    continue
                # If our string starts with an open paren
                if column[0] == "(":
                    # Assume that this column does not begin a new row.
                    new_row = False
                    # If we've been filling out a row
                    if len(latest_row) > 0:
                        # Check if the previous entry ended in a close paren. If so, the
                        # row we've been filling out has been COMPLETED as:
                        #    1) the previous entry ended in a )
                        #    2) the current entry starts with a (
                        if latest_row[-1][-1] == ")":
                            # Remove the close paren.
                            latest_row[-1] = latest_row[-1][:-1]
                            new_row = True
                    # If we've found a new row, write it out and begin our new one
                    if new_row:
                        writer.writerow(latest_row)
                        latest_row = []
                    # If we're beginning a new row, eliminate the opening parentheses.
                    if len(latest_row) == 0:
                        column = column[1:]
                # Add our column to the row we're working on.
                latest_row.append(column)
            # At the end of an INSERT statement, we'll have the semicolon. Make sure to
            # remove the semicolon and the close paren.
            if latest_row[-1][-2:] == ");":
                latest_row[-1] = latest_row[-1][:-2]
                writer.writerow(latest_row)


def recursive_filter(item, *forbidden):
    # https://stackoverflow.com/a/58838242/4100024
    # USAGE: clean = recursive_filter(dirty, *iterable_of_forbidden_things)
    if isinstance(item, list):
        return [
            recursive_filter(entry, *forbidden)
            for entry in item
            if entry not in forbidden
        ]
    if isinstance(item, dict):
        result = {}
        for key, value in item.items():
            value = recursive_filter(value, *forbidden)
            if key not in forbidden:
                result[key] = value
        return result
    return item


def track_volunteer_deletions(insert_statement):
    tmpcsv = tempfile.mkstemp()[1]
    values = insert_statement.partition("` VALUES ")[2]
    if values_sanity_check(values):
        parse_values(values, tmpcsv)
    # remove non-volunteer lines
    with open(tmpcsv, "r") as infile, open(
        f"{output_dir}/deleted_records.csv", "w"
    ) as outfile:
        writer = csv.writer(outfile)
        for line in csv.reader(infile):
            if any(volunteer in line for volunteer in volunteers):
                writer.writerow(line)
    return


def main(init: ("export initial json files only", "flag", "i")):

    # SOURCES:
    # keys are table names
    # api is the endpoint path without leading or trailing slashes
    # lock_version is the zero-indexed column number in the table
    # system_mtime is the zero-indexed column number in the table
    # total_columns is the count of the columns in the table
    # TODO check db schema version in case of incompatible upgrades
    sources = {
        "deleted_records": None,
        "agent_corporate_entity": {
            "api": "agents/corporate_entities",
            "lock_version": "1",
            "system_mtime": "7",
            "total_columns": "12",
        },
        "agent_person": {
            "api": "agents/people",
            "lock_version": "1",
            "system_mtime": "7",
            "total_columns": "12",
        },
        "archival_object": {
            "api": "repositories/2/archival_objects",
            "lock_version": "1",
            "system_mtime": "21",
            "total_columns": "27",
        },
        "resource": {
            "api": "repositories/2/resources",
            "lock_version": "1",
            "system_mtime": "30",
            "total_columns": "40",
        },
        "subject": {
            "api": "subjects",
            "lock_version": "1",
            "system_mtime": "12",
            "total_columns": "16",
        },
    }

    if not init:
        if git.Repo(os.path.abspath(output_dir)):
            git_repository = git.Repo(os.path.abspath(output_dir))
        # exit if repository is dirty
        if git_repository.is_dirty(untracked_files=True):
            sys.exit(str(datetime.today()) + " ⛔️ exited: git repository is dirty")

        mysqldump_dir = config("MYSQLDUMP_DIR")
        # files_last_modified_desc = get_files_last_modified_desc(mysqldump_dir)
        # files = {"new": files_last_modified_desc[0], "old": files_last_modified_desc[1]}
        files = {
            # "new": f"{mysqldump_dir}/caltech-2021-05-01.sql.gz",
            # "old": f"{mysqldump_dir}/caltech-2021-03-01.sql.gz",
            "new": f"{mysqldump_dir}/archivesspace-2021-05-03.sql.gz",
            "old": f"{mysqldump_dir}/archivesspace-2021-05-02.sql.gz",
            # "old": f"{mysqldump_dir}/caltech-2021-05-01.sql.gz",
        }

        tmpcsv_dir = tempfile.mkdtemp()

    git_files_add_archivists = []
    git_files_add_volunteers = []
    git_files_remove = []

    # two loops: 1- loop over each source, 2- loop over each file
    for source_table, source_info in sources.items():
        print(source_table)

        if init:
            if source_table == "deleted_records":
                Path(f"{output_dir}/deleted_records.csv").touch()
                continue
            else:
                # use `source_info['api']` to get api endpoint path (without leading/trailing slashes)
                identifiers = asnake_client.get(
                    f"/{source_info['api']}?all_ids=true"
                ).json()
        else:
            # adapted from mysqldumpgz_to_csv_tables.py
            for key, value in files.items():
                print(key, value)
                with gzip.open(files[key], "rt") as f:
                    for line in f:
                        # Look for an INSERT statement and parse it.
                        if line.startswith("INSERT INTO"):
                            # get table name
                            table = get_table_name(line)
                            if table != source_table:
                                continue
                            elif table == "deleted_records":
                                track_volunteer_deletions(line)
                            # set up output file
                            tablecsv = f"{tmpcsv_dir}/complete-{key}-{table}.csv"
                            values = line.partition("` VALUES ")[2]
                            if values_sanity_check(values):
                                parse_values(values, tablecsv)

            if source_table == "deleted_records":
                continue
            # list wanted columns
            wanted_columns = []
            for i in range(int(source_info["total_columns"])):
                wanted_columns.append(i)
            # delete in reverse order so as to not disturb the index
            # NOTE it is likely unneccessary to exclude all timestamp columns, but more
            # testing is needed to see if it would make a difference
            del wanted_columns[int(source_info["system_mtime"])]
            del wanted_columns[int(source_info["lock_version"])]
            # load csv into dataframe without unwanted columns
            complete_old = pd.read_csv(
                f"{tmpcsv_dir}/complete-old-{source_table}.csv",
                header=None,
                usecols=wanted_columns,
            )
            complete_new = pd.read_csv(
                f"{tmpcsv_dir}/complete-new-{source_table}.csv",
                header=None,
                usecols=wanted_columns,
            )
            # write out to csv
            complete_old.to_csv(
                f"{tmpcsv_dir}/partial-old-{source_table}.csv",
                mode="w",
                header=False,
                index=False,
            )
            complete_new.to_csv(
                f"{tmpcsv_dir}/partial-new-{source_table}.csv",
                mode="w",
                header=False,
                index=False,
            )

            old = open(f"{tmpcsv_dir}/partial-old-{source_table}.csv", "r").readlines()
            new = open(f"{tmpcsv_dir}/partial-new-{source_table}.csv", "r").readlines()

            # `identifiers` will be a set of all additions and modifications while
            # `subtractions` will be a set of only removed items
            identifiers = set()
            subtractions = set()
            for line in difflib.unified_diff(old, new, n=0):
                print(line)
                splat = line.split(",", 1)[0]
                if splat.startswith("+"):
                    if splat.strip("+").isnumeric():
                        identifiers.add(splat.strip("+"))
                if splat.startswith("-"):
                    if splat.strip("-").isnumeric():
                        subtractions.add(splat.strip("-"))
            # when a diff line starts with `-` and has no corresponding `+` then we know
            # to delete the record
            deletions = list(subtractions - identifiers)
            # TODO the double loop seems expensive
            for id_ in deletions:
                path = f"{output_dir}/{source_info['api']}"
                for i in os.listdir(path):
                    if os.path.isfile(os.path.join(path, i)) and i.startswith(
                        f"{id_}-"
                    ):
                        git_files_remove.append(os.path.join(path, i))
                        os.remove(os.path.join(path, i))

        if not init:
            print(identifiers)
        else:
            print(len(identifiers))

        for id_ in list(identifiers):
            # use source_info['api'] to get api endpoint path (without leading/trailing slashes)
            result = asnake_client.get(f"/{(source_info['api'])}/{id_}").json()
            # `title` is not required for an `archival_object`
            if "title" not in result:
                if "display_string" not in result:
                    print(
                        f"⚠️  missing title & display_string: /{source_info['api']}/{id_}"
                    )
                    continue
                else:
                    name = result["display_string"]
            else:
                name = result["title"]
            # delete keys that muck up diffs
            conditional_keys_to_remove = [
                "created_by",
                "last_modified_by",
            ]
            keys_to_remove = [
                "create_time",
                "lock_version",
                "system_mtime",
                "user_mtime",
            ]
            # TODO turn duplicate code into a function
            if result["last_modified_by"] not in volunteers:
                keys_to_remove.extend(conditional_keys_to_remove)
                # clean
                clean = recursive_filter(result, *keys_to_remove)
                # export JSON file
                # limit the slug to 240 characters to avoid a macOS 255-character filename limit
                # lowercase to avoid case-only filename changes that mess up git
                slug = make_safe_filename(name)[:240].lower()
                with open(
                    f"{output_dir}/{source_info['api']}/{id_}-{slug}.json",
                    "w",
                ) as f:
                    json.dump(clean, f, ensure_ascii=False, indent=4, sort_keys=True)
                    # add filename to archivists list
                    git_files_add_archivists.append(
                        f"{output_dir}/{source_info['api']}/{id_}-{slug}.json"
                    )
            else:
                # clean
                clean = recursive_filter(result, *keys_to_remove)
                # export JSON file
                # limit the slug to 240 characters to avoid a macOS 255-character filename limit
                slug = make_safe_filename(name)[:240]
                with open(
                    f"{output_dir}/{source_info['api']}/{id_}-{slug}.json",
                    "w",
                ) as f:
                    json.dump(clean, f, ensure_ascii=False, indent=4, sort_keys=True)
                    # add filename to volunteers list
                    git_files_add_volunteers.append(
                        f"{output_dir}/{source_info['api']}/{id_}-{slug}.json"
                    )

    if init:
        print("✅ baseline files generated, add & commit to git repository")
    else:
        # TODO turn duplicate code into a function

        # add archivist-modified files to git
        git_repository.index.add(git_files_add_archivists)

        # check differences between current files and last commit
        # NOTE: diff is an empty string if nothing has changed
        if git_repository.git.diff(git_repository.head.commit.tree):
            print(
                str(datetime.today())
                + " archivist changes detected; committing and pushing to remote",
                flush=True,
            )
            # commit changes when they exist
            git_repository.index.commit(
                f"🆗 archivist changes [{Path(files['new']).stem.split('.')[0]}]"
            )
            # git_repository.remotes.origin.push()
        else:
            print(str(datetime.today()) + " no archivist changes detected", flush=True)

        # add deleted files to git
        if git_files_remove:
            git_repository.index.remove(git_files_remove, working_tree=True)
            # add `deleted_records` table as csv file
            git_repository.index.add(f"{output_dir}/deleted_records.csv")
            # check differences between current files and last commit
            # NOTE: diff is an empty string if nothing has changed
            if git_repository.git.diff(git_repository.head.commit.tree):
                print(
                    str(datetime.today())
                    + " deletions detected; committing and pushing to remote",
                    flush=True,
                )
                # commit changes when they exist
                git_repository.index.commit(
                    f"👀 deletions [{Path(files['new']).stem.split('.')[0]}]"
                )
                # git_repository.remotes.origin.push()
        else:
            print(str(datetime.today()) + " no deletions detected", flush=True)

        # add volunteer-modified files to git
        from pprint import pprint

        pprint(git_files_add_volunteers)
        git_repository.index.add(git_files_add_volunteers)
        # check differences between current files and last commit
        # NOTE: diff is an empty string if nothing has changed
        if git_repository.git.diff(git_repository.head.commit.tree):
            print(
                str(datetime.today())
                + " volunteer changes detected; committing and pushing to remote",
                flush=True,
            )
            # commit changes when they exist
            git_repository.index.commit(
                f"👀 volunteer changes [{Path(files['new']).stem.split('.')[0]}]"
            )
            # git_repository.remotes.origin.push()
        else:
            print(str(datetime.today()) + " no volunteer changes detected", flush=True)


# fmt: off
if __name__ == "__main__":
    import plac; plac.call(main)
# fmt: on
