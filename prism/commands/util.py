import gzip
import os.path
import sys
import shutil
import json
import csv


def get_schema(p, file, sourceName, sourceWID):
    # Start witha blank schema definition.
    schema = {}

    # A file always takes precedence over sourceName and sourceWID
    # options, and must contain a valid schema.

    if file is not None:
        if file.lower().endswith(".json"):
            try:
                with open(file) as json_file:
                    schema = json.load(json_file)
            except Exception as e:
                print(f"Invalid schema file: {e.msg}.")
                sys.exit(1)

            # The JSON file could be a complete table definitions (GET:/tables - full) or just
            # the list of fields.  If we got a list, then we have a list of fields we
            # use to start the schema definition.

            if type(schema) is list:
                schema["fields"] = schema
            else:
                # This should be a full schema, perhaps from a table list command.
                if "name" not in schema and "fields" not in schema:
                    print("Invalid schema - name and fields attribute not found.")
                    sys.exit(1)
        elif file.lower().endswith(".csv"):
            schema = schema_from_csv(p, file)
        else:
            print("Invalid file extension - valid extensions are .json or .csv.")
            sys.exit(1)
    else:
        # No file was specified, check for a source table.

        if sourceName is None and sourceWID is None:
            print("No schema provided and a table (--sourceName or --sourceWID) not specified.")
            sys.exit(1)

        if sourceWID is not None:
            tables = p.tables_list(wid=sourceWID, type_="full")  # Exact match on WID - and get the fields
        else:
            tables = p.tables_list(name=sourceName, type_="full")  # Exact match on API Name

        if tables["total"] == 0:
            print("Invalid --sourceName or --sourceWID : table not found.")
            sys.exit(1)
        else:
            schema = tables["data"][0]

    return schema


def schema_from_csv(prism, file):
    global data_sources

    schema = {"fields": []}  # Start with an empy schema definition.

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Force all the columns names to lowercase to make lookups consistent
        # regardless of the actual case of the columns.
        reader.fieldnames = [f_name.lower() for f_name in reader.fieldnames]

        # The minimum definition is a name column - exit if not found.  No other
        # column definition is required to build a valid field list.
        if "name" not in reader.fieldnames:
            print("CSV file {file} does not contain a name column header in first line.")
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

                    if "parseformat" in row and isinstance(row["parseformat"], str) and len(row["parseformat"]) > 0:
                        field["parseFormat"] = row["parseformat"]

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
        print("An existing file name is required to upload to a file container.")
        return None

    # Verify that each file is already a gzip file or a CSV we gzip for them.

    # The CSV contents are not validated here - Prism eventually
    # returns an error if the content is invalid.

    target_files = []

    for f in file:
        target_file = file

        if f.lower().endswith(".csv"):
            # GZIP the file into the same directory with the appropriate extension.
            target_file = f + ".gz"

            with open(f, 'rb') as f_in:
                with gzip.open(target_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
        elif not f.lower().endswith(".gz"):
            print(f"File {f} is not a .gz or .csv file.")
            return None

        target_files.append(target_file)

    # Assume we have a fID - it can be None right now
    # if the user wants to create a fileContainers during
    # this operation.
    fID = filecontainersid

    for target_file in target_files:
        # Load the file and retrieve the fID - this is only
        # set by the load on the first file - subsequent
        # files are loaded into the same container (fID).
        fID = prism.filecontainers_load(fID, target_file, )

        # If the fID comes back blank, then something is not
        # working.  Note: any error messages have already
        # been displayed by the load operation.
        if fID is None:
            break

    # Return the fID to the caller - this is the value
    # passed by the caller, or the new fID created by
    # the load of the first file.
    return fID


def compress_files(files):
    target_files = []

    if files is None:
        print("File(s) must be specified.")
        return target_files
    elif isinstance(files, list) and len(files) == 0:
        print("File(s) must be specified.")
        return target_files
    elif isinstance(files, str) and not files:
        print("File(s) must be specified.")
        return target_files

    if isinstance(files, str):
        files = [ files ]

    for f in files:
        if not os.path.exists(f):
            print(f"FIle {f} not found - skipping.")
            continue

        if f.lower().endswith(".csv"):
            # GZIP the file into the same directory with the appropriate extension.
            target_file = f + ".gz"

            with open(f, 'rb') as f_in:
                with gzip.open(target_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            target_files.append(target_file)
        elif f.lower().endswith(".gz"):
            target_files.append(f)
        else:
            print(f"File {f} is not a .gz or .csv file - skipping.")

    return target_files
