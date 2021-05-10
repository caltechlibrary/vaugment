import csv
import difflib
import gzip
import json
import os
import pandas as pd
from pathlib import Path
import shutil
import sys

from asnake.client import ASnakeClient

asnake_client = ASnakeClient()
asnake_client.authorize()

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


def main(init: ("export initial json files only", "flag", "i")):

    # SOURCES:
    # keys are table names
    # api is the endpoint path without leading or trailing slashes
    # lock_version is the zero-indexed column number in the table
    # system_mtime is the zero-indexed column number in the table
    # total_columns is the count of the columns in the table
    sources = {
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
        # "archival_object": {
        #     "api": "repositories/2/archival_objects",
        #     "lock_version": "1",
        #     "system_mtime": "21",
        #     "total_columns": "27",
        # },
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
        # TODO create settings.ini file for file paths
        # files_last_modified_desc = get_files_last_modified_desc(
        #     "/Users/tkeswick/Development/archives/data/lyrasis"
        # )
        # files = {"new": files_last_modified_desc[0], "old": files_last_modified_desc[1]}
        files = {
            # "new": "/Users/tkeswick/Development/archives/data/lyrasis/caltech-2021-05-01.sql.gz",
            # "old": "/Users/tkeswick/Development/archives/data/lyrasis/caltech-2021-03-01.sql.gz",
            "new": "/Users/tkeswick/Development/archives/data/lyrasis/archivesspace-2021-05-06.sql.gz",
            "old": "/Users/tkeswick/Development/archives/data/lyrasis/caltech-2021-05-01.sql.gz",
        }

        output_dir = Path(f"/tmp/archivesspace-csv-records")
        if Path(output_dir).is_dir():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    # two loops: 1- loop over each source, 2- loop over each file

    for source_table, source_info in sources.items():
        print(source_table)

        if init:
            # use `source_info['api']` to get api endpoint path (without leading/trailing slashes)
            identifiers = asnake_client.get(
                f"/{source_info['api']}?all_ids=true"
            ).json()
        else:
            # adapted from mysqldumpgz_to_csv_tables.py
            for key, value in files.items():
                print(key)
                print(value)
                with gzip.open(files[key], "rt") as f:
                    for line in f:
                        # Look for an INSERT statement and parse it.
                        if line.startswith("INSERT INTO"):
                            # get table name
                            table = get_table_name(line)
                            if table != source_table:
                                continue
                            # set up output file
                            tablecsv = f"{output_dir}/complete-{key}-{table}.csv"
                            values = line.partition("` VALUES ")[2]
                            if values_sanity_check(values):
                                parse_values(values, tablecsv)

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
                f"/tmp/archivesspace-csv-records/complete-old-{source_table}.csv",
                header=None,
                usecols=wanted_columns,
            )
            complete_new = pd.read_csv(
                f"/tmp/archivesspace-csv-records/complete-new-{source_table}.csv",
                header=None,
                usecols=wanted_columns,
            )
            # write out to csv
            complete_old.to_csv(
                f"/tmp/archivesspace-csv-records/partial-old-{source_table}.csv",
                mode="w",
                header=False,
                index=False,
            )
            complete_new.to_csv(
                f"/tmp/archivesspace-csv-records/partial-new-{source_table}.csv",
                mode="w",
                header=False,
                index=False,
            )

            old = open(
                f"/tmp/archivesspace-csv-records/partial-old-{source_table}.csv", "r"
            ).readlines()
            new = open(
                f"/tmp/archivesspace-csv-records/partial-new-{source_table}.csv", "r"
            ).readlines()

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
                # TODO set path in settings
                path = f"/tmp/archivesspace-json-records/{source_info['api']}"
                for i in os.listdir(path):
                    if os.path.isfile(os.path.join(path, i)) and i.startswith(
                        f"{id_}-"
                    ):
                        os.remove(os.path.join(path, i))

        # use source_info['api'] to get api endpoint path (without leading/trailing slashes)
        print(identifiers)
        for id_ in list(identifiers):
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
            volunteers = [
                "alexandra",
                "kitty",
                "mary",
            ]
            keys_to_remove = [
                "create_time",
                "lock_version",
                "system_mtime",
                "user_mtime",
            ]
            if result["last_modified_by"] not in volunteers:
                keys_to_remove.extend(conditional_keys_to_remove)
            clean = recursive_filter(result, *keys_to_remove)
            # limit the slug to 240 characters to avoid a macOS 255-character filename limit
            slug = make_safe_filename(name)[:240]
            with open(
                # TODO set path in settings
                f"/tmp/archivesspace-json-records/{source_info['api']}/{id_}-{slug}.json",
                "w",
            ) as f:
                json.dump(clean, f, ensure_ascii=False, indent=4, sort_keys=True)


# fmt: off
if __name__ == "__main__":
    import plac; plac.call(main)
# fmt: on
