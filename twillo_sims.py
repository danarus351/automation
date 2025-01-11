from twilio.rest import Client
import json
import pandas as pd
from tqdm import tqdm

with open('<twillio api key>') as json_file:
    data = json.load(json_file)
    account_sid = data['sid']
    auth_token = data['token']

client = Client(account_sid, auth_token)

sims = client.wireless.v1.sims.list()
# start_date = datetime(2023, 1, 1)  # Example: January 1, 2023
# end_date = datetime(2024, 3, 4)
active_sim = 0
recordes = []
for sim in tqdm(sims):
    try:
        last_usage = (client.wireless.v1.sims(sim.sid).data_sessions.list(limit=1))[0].last_updated
        active_sim += 1
    except IndexError:
        last_usage = 'not used in the last year'
    rec = {"SID": sim.sid, "Unique Name": sim.unique_name, "Status": sim.status, "sim_created": sim.date_created , "last_usage": last_usage, "sim_iccid": sim.iccid, "imei": sim.unique_name }
    recordes.append(rec)
df = pd.DataFrame(recordes).to_csv('<csv file for data dump>', index=False)
print(f'number of devices active in the last year: {active_sim}')