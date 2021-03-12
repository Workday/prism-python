import os
import prism

# instantiate the Prism class
p = prism.Prism(
    os.getenv("workday_base_url"),
    os.getenv("workday_tenant_name"),
    os.getenv("prism_client_id"),
    os.getenv("prism_client_secret"),
    os.getenv("prism_refresh_token"),
)

# load schema for new table
schema = prism.load_schema("schema.json")

# create the table in Prism
table = prism.create_table(p, "Topic_Model_Predictions_BDS", schema["fields"])

# upload the file to the table
prism.upload_file(p, "predictions.csv.gz", table["id"], operation="TruncateandInsert")
