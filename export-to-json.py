import json

from asnake.client import ASnakeClient

asnake_client = ASnakeClient()
asnake_client.authorize()

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


def make_safe_filename(s):
    def safe_char(c):
        if c.isalnum():
            return c
        else:
            return "_"

    return "".join(safe_char(c) for c in s).rstrip("_")


for source_table, source_info in sources.items():
    print(source_table)

    # use `source_info['api']` to get api endpoint path (without leading/trailing slashes)
    identifiers = asnake_client.get(f"/{source_info['api']}?all_ids=true").json()

    for id_ in list(identifiers):
        result = asnake_client.get(f"/{source_info['api']}/{id_}").json()
        # `title` is not required for an `archival_object`
        if "title" not in result:
            if "display_string" not in result:
                print(f"⚠️  missing title & display_string: /{source_info['api']}/{id_}")
                continue
            else:
                name = result["display_string"]
        else:
            name = result["title"]
        # delete items that are irrelevant in diffs
        if "lock_version" in result:
            del result["lock_version"]
        if "system_mtime" in result:
            del result["system_mtime"]
        # limit the slug to 240 characters to avoid a macOS 255-character filename limit
        slug = make_safe_filename(name)[:240]
        with open(
            f"/tmp/archivesspace-json-records/{source_info['api']}/{id_}-{slug}.json",
            "w",
        ) as f:
            json.dump(result, f, ensure_ascii=False, indent=4, sort_keys=True)


# example result
"""
{
    "agent_contacts": [
        {
            "address_1": "1200 East California Blvd.",
            "address_2": "MC B215-74",
            "city": "Pasadena",
            "country": "United States of America",
            "create_time": "2020-08-12T23:49:36Z",
            "created_by": "pcollopy",
            "email": "archives@caltech.edu",
            "jsonmodel_type": "agent_contact",
            "last_modified_by": "pcollopy",
            "lock_version": 0,
            "name": "California Institute of Technology Archives and Special Collections",
            "post_code": "91125",
            "region": "California",
            "system_mtime": "2020-08-12T23:49:36Z",
            "telephones": [
                {
                    "create_time": "2020-08-12T23:49:36Z",
                    "created_by": "pcollopy",
                    "jsonmodel_type": "telephone",
                    "last_modified_by": "pcollopy",
                    "number": "(626) 395-2704",
                    "number_type": "business",
                    "system_mtime": "2020-08-12T23:49:36Z",
                    "uri": "/telephone/11",
                    "user_mtime": "2020-08-12T23:49:36Z"
                }
            ],
            "user_mtime": "2020-08-12T23:49:36Z"
        }
    ],
    "agent_type": "agent_corporate_entity",
    "create_time": "2015-10-19T22:36:12Z",
    "created_by": "admin",
    "dates_of_existence": [
        {
            "begin": "1968",
            "create_time": "2020-08-12T23:49:36Z",
            "created_by": "pcollopy",
            "date_type": "range",
            "jsonmodel_type": "date",
            "label": "existence",
            "last_modified_by": "pcollopy",
            "lock_version": 0,
            "system_mtime": "2020-08-12T23:49:36Z",
            "user_mtime": "2020-08-12T23:49:36Z"
        }
    ],
    "display_name": {
        "authorized": true,
        "create_time": "2020-08-12T23:49:36Z",
        "created_by": "pcollopy",
        "is_display_name": true,
        "jsonmodel_type": "name_corporate_entity",
        "last_modified_by": "pcollopy",
        "lock_version": 0,
        "primary_name": "California Institute of Technology Archives and Special Collections",
        "sort_name": "California Institute of Technology Archives and Special Collections",
        "sort_name_auto_generate": true,
        "source": "local",
        "system_mtime": "2020-08-12T23:49:36Z",
        "use_dates": [],
        "user_mtime": "2020-08-12T23:49:36Z"
    },
    "external_documents": [],
    "is_linked_to_published_record": false,
    "is_slug_auto": false,
    "jsonmodel_type": "agent_corporate_entity",
    "last_modified_by": "pcollopy",
    "linked_agent_roles": [],
    "lock_version": 26,
    "names": [
        {
            "authorized": true,
            "create_time": "2020-08-12T23:49:36Z",
            "created_by": "pcollopy",
            "is_display_name": true,
            "jsonmodel_type": "name_corporate_entity",
            "last_modified_by": "pcollopy",
            "lock_version": 0,
            "primary_name": "California Institute of Technology Archives and Special Collections",
            "sort_name": "California Institute of Technology Archives and Special Collections",
            "sort_name_auto_generate": true,
            "source": "local",
            "system_mtime": "2020-08-12T23:49:36Z",
            "use_dates": [],
            "user_mtime": "2020-08-12T23:49:36Z"
        }
    ],
    "notes": [],
    "publish": false,
    "related_agents": [
        {
            "create_time": "2020-08-12 23:49:36 UTC",
            "created_by": "pcollopy",
            "jsonmodel_type": "agent_relationship_associative",
            "last_modified_by": "pcollopy",
            "ref": "/agents/people/3090",
            "relator": "is_associative_with",
            "system_mtime": "2020-08-12 23:49:36 UTC",
            "user_mtime": "2020-08-12 23:49:36 UTC"
        },
        {
            "create_time": "2020-08-12 23:49:36 UTC",
            "created_by": "pcollopy",
            "jsonmodel_type": "agent_relationship_subordinatesuperior",
            "last_modified_by": "pcollopy",
            "ref": "/agents/corporate_entities/181",
            "relator": "is_subordinate_to",
            "system_mtime": "2020-08-12 23:49:36 UTC",
            "user_mtime": "2020-08-12 23:49:36 UTC"
        }
    ],
    "system_mtime": "2020-08-12T23:49:36Z",
    "title": "California Institute of Technology Archives and Special Collections",
    "uri": "/agents/corporate_entities/1",
    "used_within_published_repositories": [],
    "used_within_repositories": [],
    "user_mtime": "2020-08-12T23:49:36Z"
}
"""
