import logging
import os.path


def get_files(files):
    """Evaluate one (str) or more (list) file names and return a valid list for load operations."""

    # At a minimum, an empty list will always be returned.
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
