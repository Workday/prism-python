import click
import json
import pandas as pd

# Lazy instantiation of sources for Instance type columns (if any) for a create operation.
data_sources = None


@click.command("list", help="View the tables or datasets permitted by the security profile of the current user.")
@click.option("-w", "--wid",
              help="Unique WID for Prism table or dataset.")
@click.option("-l", "--limit",
              help="The maximum number of object data entries included in the response, default=all.",
              type=int,
              default=None)
@click.option("-o", "--offset",
              help="The offset to the first object in a collection to include in the response.",
              type=int,
              default=None)
@click.option("-t", "--type", "type_",
              help="How much information to be returned in response JSON.",
              type=click.Choice(["summary", "full", "permissions"], case_sensitive=False),
              default="summary")
@click.option("-f", "--format", "format_",
              default="json",
              help="Format output as JSON, summary, schema, or CSV.",
              type=click.Choice(['json', 'summary', 'schema', 'csv'], case_sensitive=False))
@click.option("-s", "--search",
              help="Search substring in api name or display name (default=false).",
              is_flag=True)
@click.argument("api_name", required=False)
@click.pass_context
def tables_list(ctx, api_name, wid, limit, offset, type_, format_, search):
    """tables list TABLENAME

    Prism API TABLENAME of the table to list.
    """

    if type_ in ("summary", "permissions") and format in ("schema", "csv"):
        # Summary results cannot generate schema or CSV output.

        print(f"Invalid combination of type {type_} and format {format}.")
        return

    p = ctx.obj["p"]

    # Query the tenant...
    tables = p.tables_list(api_name, wid, limit, offset, type_, search)

    # The return always has a total tables returned value.
    # note: tables_list never fails, it simply returns 0 tables if there is a problem.
    if tables["total"] == 0:
        return

    # Handle output
    if format_ == "json":
        # The results could be one or more tables - simply dump the
        # returned object.

        print(json.dumps(tables, indent=2))
    elif format_ == "summary":
        for table in tables["data"]:
            print(f'{table["displayName"]}, Rows: {table["stats"]["rows"]}, Size: {table["stats"]["rows"]}, Refreshed: {table["dateRefreshed"]}')
    elif format_ == "csv":
        df = pd.json_normalize(tables["data"])
        print(df.to_csv())
    elif format_ == "schema":
        # Dump out the fields of the first table in the result in
        # a format compatible with a schema used to created or edit
        # a table.
        table = tables["data"][0]  # Only output the first table.

        # Remove the Prism audit columns.
        fields = [fld for fld in tables["data"][0]["fields"] if not fld["name"].startswith("WPA_")]

        # Remove tenant specific values - these are not needed
        # if the user wants to update a table definition.

        for fld in fields:
            if "fieldId" in fld:
                if "fieldId" in fld:
                    del fld["fieldId"]

                if "id" in fld:
                    del fld["id"]

        print(json.dumps(fields, indent=2))
    else:
        table = tables["data"][0]  # Only output the first table.
        fields = [fld for fld in tables["data"][0]["fields"] if not fld["name"].startswith("WPA_")]

        print(csv_from_fields(fields))


@click.command("create", help="Create a new table with the specified name.")
@click.option("-d", "--displayName", help="Specify a display name - defaults to name")
@click.option("-e", "--enabledForAnalysis", is_flag=True, default=False, help="Enable this table for analytics.")
@click.option("-n", "--sourceName", help="The API name of an existing table to copy.")
@click.option("-w", "--sourceWID", help="The WID of an existing table to copy.")
@click.argument("name", required=True)
@click.argument("file", required=False, type=click.Path(exists=True))
@click.pass_context
def tables_create(ctx, displayname, enabledforanalysis, sourcename, sourcewid, format_, name, file):
    p = ctx.obj["p"]

    if file is not None:
        if file.lower().endswith(".json"):
            schema = json.loads(file.read())

            # The JSON file could be a complete table definitions (GET:/tables - full) or just
            # the list of fields.  If we got a list, then we have a list of fields we
            # use to start the schema definition.

            if type(schema) is list:
                fields = schema
            else:
                fields = schema["fields"]
        elif file.lower().endswith(".csv"):
            fields = fields_from_csv(p, file)
        else:
            print("Invalid file extension - valid extensions are .json or .csv.")
            return
    else:
        if sourcename is None and sourcewid is None:
            print("No schema provided and a table to copy (--sourceName or --sourceWID) not specified.")
            return

        if sourcewid is not None:
            tables = p.tables_list(wid=sourcewid, type_="full")  # Exact match on WID - and get the fields
        else:
            tables = p.tables_list(name=sourcename, type_="full")  # Exact match on API Name

        if tables["total"] == 0:
            print("Invalid --sourceName or --sourceWID : table not found.")
            return

        fields = tables["data"][0]["fields"]

    fields[:] = [fld for fld in fields if "WPA" not in fld["name"]]

    # Initialize a new schema with just the fields.
    schema = {"fields": fields}

    # Set the particulars for this table operation.

    schema["enableForAnalysis"] = enabledforanalysis
    schema["name"] = name.replace(" ", "_")  # Minor clean-up

    if displayname is not None:
        schema["displayName"] = displayname
    else:
        schema["displayName"] = name

    table_def = p.tables_create(schema["name"], schema)

    if table_def is not None:
        print(f"Table {name} created.")


@click.command("update", help="Edit the schema for an existing table.")
@click.option("-s", "--sourceName", help="The API name of an existing table to copy.")
@click.option("-w", "--sourceWID", help="The ID of an existing table to copy.")
@click.argument("name", required=True)
@click.argument("file", required=False, type=click.Path(exists=True))
@click.pass_context
def tables_update(ctx, name, filename, source_table, source_id):
    p = ctx.obj["p"]

    table = p.tables(name=name)

    if table is not None:
        p.tables_put(name, filename)

    print("update")


@click.command("upload",
               help="Upload a file into the table using a bucket.")
@click.option("-n", "--table_name",
              help="Specify a name for the table.")
@click.option("-i", "--table_id",
              help="Specify a specific table API ID - this value overrides a name, if specified.")
@click.option("-b", "--bucket",
              help="Specify a bucket name - defaults to random bucket name.")
@click.option("-o", "--operation", default="TruncateandInsert",
              help="Operation for the bucket - default to TruncateandInsert.")
@click.option("-f", "--filename",
              help="File (csv or gzip) to upload.")
@click.pass_context
def tables_upload(ctx, table_name, table_id, bucket, operation, filename):
    p = ctx.obj["p"]

    print("upload")


def csv_from_fields(fields):
    format_str = '{name},"{displayName}",{ordinal},{type},"{businessObject}",{precision},{scale},"{parseFormat}",{required},{externalId}\n'

    csv = "name,displayName,ordinal,type,businessObject,precision,scale,parseFormat,required,externalId\n"

    for field in fields:
        # Suppress Prism auditing fields.
        if field["name"].startswith("WPA_"):
            continue

        field_def = {"name": field["name"],
                     "displayName": field["displayName"],
                     "ordinal": field["ordinal"],
                     "type": field["type"]["descriptor"],
                     "businessObject": field["businessObject"]["descriptor"] if "businessObject" in field else "",
                     "precision": field["precision"] if "precision" in field else "",
                     "scale": field["scale"] if "scale" in field else "",
                     "parseFormat": field["parseFormat"] if "parseFormat" in field else "",
                     "required": field["required"],
                     "externalId": field["externalId"]
                     }

        csv += format_str.format_map(field_def)

    return csv


def fields_from_csv(prism, file):
    global data_sources

    schema = {"fields": []}  # Start with an empy schema definition.

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        ordinal = 1

        for row in reader:
            field = {
                "ordinal": ordinal,
                "name": row["name"],
                "displayName": row["displayName"] if "displayName" in row else row["name"],
                "required": row["required"] if "required" in row else False,
                "externalId": row["externalId"] if "externalId" in row else False
            }

            match row["type"].lower():
                case "text":
                    field["type"] = {
                        "id": "fdd7dd26156610006a12d4fd1ea300ce",
                        "descriptor": "Text"
                    }
                case "date":
                    field["type"] = {
                        "id": "fdd7dd26156610006a71e070b08200d6",
                        "descriptor": "Date"
                    }

                    if "parseFormat" in row:
                        field["parseFormat"] = row["parseFormat"]

                case "numeric":
                    field["type"] = {
                        "id": "32e3fa0dd9ea1000072bac410415127a",
                        "descriptor": "Numeric"
                    }

                    if "precision" in row:
                        field["precision"] = row["precision"]

                        if "scale" in row:
                            field["scale"] = row["scale"]

                case "instance":
                    # We need all the data sources to resolve the business objects
                    # to include their WID.
                    if data_sources is None:
                        data_sources = prism.datasources_list()

                        if data_sources is None or data_sources["total"] == 0:
                            print("Error calling WQL/dataSources")
                            return

                    field["type"] = {
                        "id": "db9cd1dbf95010000e8fc7c78cd012a9",
                        "descriptor": "Instance"
                    }

                    # Find the matching businessObject
                    bo = [ds for ds in data_sources["data"]
                               if ds["businessObject"]["descriptor"] == row["businessObject"]]

                    if len(bo) == 1:
                        field["businessObject"] = bo[0]["businessObject"]
                case _:
                    # Default to string
                    field["type"] = {
                        "id": "fdd7dd26156610006a12d4fd1ea300ce",
                        "descriptor": "Text"
                    }

            schema["fields"].append(field)
            ordinal += 1

    return schema
