import json

from asnake.client import ASnakeClient

asnake_client = ASnakeClient()
asnake_client.authorize()

archival_object_id_list = asnake_client.get(
    "/repositories/2/archival_objects?all_ids=true"
).json()

# print(json.dumps(archival_object_id_list, sort_keys=True, indent=4))
# exit()

# # see an example
# archival_object = asnake_client.get(f"/archival_objects/730").json()
# print(json.dumps(archival_object, sort_keys=True, indent=4))
# exit()

for archival_object_id in archival_object_id_list:
    archival_object = asnake_client.get(
        f"/repositories/2/archival_objects/{archival_object_id}"
    ).json()
    # TODO set up plac for parameters
    with open(
        f"/tmp/archivesspace-records/archival-objects/archival-object-{archival_object_id}.json",
        "w",
    ) as f:
        json.dump(archival_object, f, ensure_ascii=False, indent=4, sort_keys=True)
