import os
import csv


def get_csv_headers(dir_path):
    """
    Finds all CSV files in the directory, reads their headers,
    and returns a dictionary mapping their paths to header lists.

    Args:
        dir_path (str): Directory path to search for CSV files.

    Returns:
        dict: {csv_filepath: {"header": [col1, col2, ...]}, ...}
    """
    header_dict = {}
    for fname in os.listdir(dir_path):
        if fname.lower().endswith(".csv"):
            full_path = os.path.join(dir_path, fname)
            try:
                with open(full_path, "r", newline="", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    header_dict[full_path] = {"header": headers}
            except Exception as e:
                # Optional: handle files that may not have a header gracefully
                header_dict[full_path] = {"header": [], "error": str(e)}
    return header_dict


if __name__ == "__main__":
    result = get_csv_headers("data")
    print(result)
