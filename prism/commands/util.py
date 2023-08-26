import gzip
import shutil


def fileContainers_load_impl(prism, filecontainersid, file):
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
