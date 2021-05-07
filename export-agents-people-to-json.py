import json

from asnake.client import ASnakeClient

asnake_client = ASnakeClient()
asnake_client.authorize()

person_id_list = asnake_client.get("/agents/people?all_ids=true").json()

# print(json.dumps(person_id_list, sort_keys=True, indent=4))
# exit()

# see an example
person = asnake_client.get(f"/agents/people/3125").json()
print(json.dumps(person, sort_keys=True, indent=4))
exit()

for person_id in person_id_list:
    person = asnake_client.get(f"/agents/people/{person_id}").json()
    # TODO set up plac for parameters
    with open(f"/tmp/archivesspace-records/agents/person-{person_id}.json", "w") as f:
        json.dump(person, f, ensure_ascii=False, indent=4, sort_keys=True)
