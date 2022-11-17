import shutil
import argparse
import os

def _to_dict(csv_file):
    """
    turns mass find and replace into dictionary object
    """
    import csv

    dict_from_csv = {}
    with open(csv_file, mode='r') as f:
        reader = csv.reader(f)
        # assuming that each row is "old address, new address" for a user
        dict_from_csv = {rows[0]:rows[1] for rows in reader}

    return dict_from_csv

def _map(file_name, mapping):
    """
    reads and replaces according to mapping
    """
    with open(file_name, "r") as f:
        data = f.read()

    for e in mapping:
        data = data.replace(e, mapping[e])
    return data

def _write(file_name, data_write):
    """
    summary: writes replaced data to same location
    """
    with open(file_name, "w") as f:
        f.write(data_write)

def _mapping_file(file_name, mapping):
    """
    maps a single file
    """
    data = _map(file_name, mapping)
    _write(file_name, data)

def _mapping_folder(folder, mapping, original_directory):
    working_dir = original_directory + '/' + folder if folder is not None else original_directory    
    print("Current working directory is:", working_dir)

    files_in_folder = os.listdir(working_dir)
    print(files_in_folder)
    for file in files_in_folder:
        if "." not in file: #assuming extension is not there -> folder
            _mapping_folder(file, mapping, working_dir)
        else:
            print("- working on file: ", file)
            try:
                _mapping_file(working_dir + '/' + file, mapping)
            except Exception as e:
                print(e)
    print("Finished working in directory:", working_dir)
    return True

def main():
    all_args = argparse.ArgumentParser()
    all_args.add_argument("--dir", dest="directory", required=True, help='directory to be mass found + replaced' )
    all_args.add_argument("-m", "--mapping", dest="mapping", required=True, help='one-to-one mapping provided by a comma delim file')

    args = all_args.parse_args()
    mapping_file = args.mapping
    directory = args.directory

    mapping = _to_dict(mapping_file)


    success = _mapping_folder(None, mapping, directory)
    if success:
        print("All files were succesfully mapped!")




if __name__ == "__main__":
    main()
