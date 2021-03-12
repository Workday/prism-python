import csv
import logging
import os
import requests

# configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# download Workday data from RaaS
r = requests.get(
    os.getenv("workday_raas_url"),
    auth=(os.getenv("workday_username"), os.getenv("workday_password")),
)

# f the request was successful, write data to CSV file
if r.status_code == 200:
    data = r.json()["Report_Entry"]
    fname = "survey_responses.csv"
    with open(fname, "w") as f:
        writer = csv.DictWriter(f, data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        logging.info(f"{fname} created")
else:
    logging.warning(f"Request not successful ({r.status_code})")
