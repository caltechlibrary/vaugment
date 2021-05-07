import json

from asnake.client import ASnakeClient

asnake_client = ASnakeClient()
asnake_client.authorize()

resource_id_list = asnake_client.get("/repositories/2/resources?all_ids=true").json()

# print(json.dumps(resource_id_list, sort_keys=True, indent=4))
# exit()

# # see an example
# resource = asnake_client.get(f"/resources/730").json()
# print(json.dumps(resource, sort_keys=True, indent=4))
# exit()

for resource_id in resource_id_list:
    resource = asnake_client.get(f"/repositories/2/resources/{resource_id}").json()
    # TODO set up plac for parameters
    with open(
        f"/tmp/archivesspace-records/resources/resource-{resource_id}.json", "w"
    ) as f:
        json.dump(resource, f, ensure_ascii=False, indent=4, sort_keys=True)
