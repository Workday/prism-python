import os.path
import sys
import json
import csv
import click
import logging


def get_schema(p, file, sourceName, sourceWID):
    # Start with a blank schema definition.
    schema = {}

    # A file always takes precedence over sourceName and sourceWID
    # options, and must contain a valid schema.

    if file is not None:
        if file.lower().endswith(".json"):
            try:
                with open(file) as json_file:
                    schema = json.load(json_file)
            except Exception as e:
                click.echo(f"Invalid schema file: {e.msg}.")
                sys.exit(1)

            # The JSON file could be a complete table definitions (GET:/tables - full) or just
            # the list of fields.  If we got a list, then we have a list of fields we
            # use to start the schema definition.

            if type(schema) is list:
                schema["fields"] = schema
            else:
                # This should be a full schema, perhaps from a table list command.
                if "name" not in schema and "fields" not in schema:
                    click.echo("Invalid schema - name and fields attribute not found.")
                    sys.exit(1)
        elif file.lower().endswith(".csv"):
            schema = schema_from_csv(p, file)
        else:
            click.echo("Invalid file extension - valid extensions are .json or .csv.")
            sys.exit(1)
    else:
        # No file was specified, check for a source table.

        if sourceName is None and sourceWID is None:
            click.echo("No schema provided and a table (--sourceName or --sourceWID) not specified.")
            sys.exit(1)

        if sourceWID is not None:
            tables = p.tables_list(wid=sourceWID, type_="full")  # Exact match on WID - and get the fields
        else:
            tables = p.tables_list(name=sourceName, type_="full")  # Exact match on API Name

        if tables["total"] == 0:
            click.echo("Invalid --sourceName or --sourceWID : table not found.")
            sys.exit(1)
        else:
            schema = tables["data"][0]

    return schema


def schema_from_csv(prism, file):
    schema = {"fields": []}  # Start with an empy schema definition.

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Force all the columns names to lowercase to make lookups consistent
        # regardless of the actual case of the columns.
        reader.fieldnames = [f_name.lower() for f_name in reader.fieldnames]

        # The minimum definition is a name column - exit if not found.  No other
        # column definition is required to build a valid field list.
        if "name" not in reader.fieldnames:
            click.echo(f"CSV file {file} does not contain a name column header in first line.")
            sys.exit(1)

        # Prism fields always have an ordinal sequence assigned to each field.
        ordinal = 1

        for row in reader:
            field = {
                "ordinal": ordinal,
                "name": row["name"],
                "displayName": row["displayname"] if "displayname" in row else row["name"]
            }

            if "required" in row and isinstance(row["required"], str) and row["required"].lower() == "true":
                field["required"] = True
            else:
                field["required"] = False

            if "externalid" in row and isinstance(row["externalid"], str) and row["externalid"].lower() == "true":
                field["externalId"] = True
            else:
                field["externalId"] = False

            fld_type = "none"

            if "type" in row and row["type"] in ["text", "date", "numeric", "instance"]:
                field["type"] = { "id" : f'Schema_Field_Type={row["type"]}'}
                fld_type = row["type"].lower()
            else:
                field["type"] = { "id" : f'Schema_Field_Type=Text'}

            match fld_type:
                case "date":
                    if "parseformat" in row and isinstance(row["parseformat"], str) and len(row["parseformat"]) > 0:
                        field["parseFormat"] = row["parseformat"]
                    else:
                        field["parseFormat"] = "yyyy-MM-dd"

                case "numeric":
                    if "precision" in row:
                        field["precision"] = row["precision"]

                        if "scale" in row:
                            field["scale"] = row["scale"]

                case "instance":
                    # We need all the data sources to resolve the business objects
                    # to include their WID.
                    data_sources = prism.datasources_list()

                    if data_sources is None or data_sources["total"] == 0:
                        click.echo("Error calling WQL/dataSources")
                        return

                    # Find the matching businessObject
                    bo = [ds for ds in data_sources["data"]
                          if ds["businessObject"]["descriptor"] == row["businessObject"]]

                    if len(bo) == 1:
                        field["businessObject"] = bo[0]["businessObject"]

            schema["fields"].append(field)
            ordinal += 1

    return schema


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


def fileContainers_load(prism, filecontainersid, file):
    # Because Click verified the file already exists, we know we have valid
    # file name.  Check to see if we have a gzip file or a CSV
    # by checking the extension.

    if file is None or len(file) == 0:
        click.echo("An existing file name is required to upload to a file container.")
        return None

    # Verify that each file is already a gzip file or a CSV we gzip for them.

    # The CSV contents are not validated here - Prism eventually
    # returns an error if the content is invalid.

    target_files = compress_files(file)

    # Assume we have a fID - it can be None right now
    # if the user wants to create a fileContainers during
    # this operation.
    fID = filecontainersid

    for target_file in target_files:
        # Load the file and retrieve the fID - this is only
        # set by the load on the first file - subsequent
        # files are loaded into the same container (fID).
        fID = prism.filecontainers_load(fID, target_file)

        # If the fID comes back blank, then something is not
        # working.  Note: any error messages have already
        # been displayed by the load operation.

        # NOTE: this operation never fails, the file is skipped.
        if fID is None:
            break

    # Return the fID to the caller - this is the value
    # passed by the caller, or the new fID created by
    # the load of the first file.
    return fID


def get_files(files):
    target_files = []

    if files is None:
        logging.getLogger("prismCLI").warning("File(s) must be specified.")
        return target_files
    elif isinstance(files, list) and len(files) == 0:
        logging.getLogger("prismCLI").warning("File(s) must be specified.")
        return target_files
    elif isinstance(files, tuple) and len(files) == 0:
        logging.getLogger("prismCLI").warning("File(s) must be specified.")
        return target_files
    elif isinstance(files, str):
        if not files:
            logging.getLogger("prismCLI").warning("File(s) must be specified.")
            return target_files
        else:
            files = [ files ]

    for f in files:
        if not os.path.exists(f):
            logging.getLogger("prismCLI").warning(f"FIle {f} not found - skipping.")
            continue

        if f.lower().endswith(".csv") or f.lower().endswith(".csv.gz"):
            target_files.append(f)
        else:
            logging.getLogger("prismCLI").warning(f"File {f} is not a .gz or .csv file - skipping.")

    return target_files
