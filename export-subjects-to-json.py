import json

from asnake.client import ASnakeClient

asnake_client = ASnakeClient()
asnake_client.authorize()

subject_id_list = asnake_client.get("/subjects?all_ids=true").json()

# print(json.dumps(subject_id_list, sort_keys=True, indent=4))
# exit()

# # see an example
# subject = asnake_client.get(f"/subjects/730").json()
# print(json.dumps(subject, sort_keys=True, indent=4))
# exit()

for subject_id in subject_id_list:
    subject = asnake_client.get(f"/subjects/{subject_id}").json()
    # TODO set up plac for parameters
    with open(
        f"/tmp/archivesspace-records/subjects/subject-{subject_id}.json", "w"
    ) as f:
        json.dump(subject, f, ensure_ascii=False, indent=4, sort_keys=True)
