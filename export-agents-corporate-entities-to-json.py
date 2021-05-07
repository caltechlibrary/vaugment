import json

from asnake.client import ASnakeClient

asnake_client = ASnakeClient()
asnake_client.authorize()

corporate_entity_id_list = asnake_client.get(
    "/agents/corporate_entities?all_ids=true"
).json()

# print(json.dumps(corporate_entity_id_list, sort_keys=True, indent=4))
# exit()

# # see an example
# corporate_entity = asnake_client.get(f"/agents/corporate_entities/730").json()
# print(json.dumps(corporate_entity, sort_keys=True, indent=4))
# exit()

for corporate_entity_id in corporate_entity_id_list:
    corporate_entity = asnake_client.get(
        f"/agents/corporate_entities/{corporate_entity_id}"
    ).json()
    # TODO set up plac for parameters
    with open(
        f"/tmp/archivesspace-records/agents/corporate-entity-{corporate_entity_id}.json",
        "w",
    ) as f:
        json.dump(corporate_entity, f, ensure_ascii=False, indent=4, sort_keys=True)

# example result
'''
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
'''
