import json
import pandas as pd
import argparse 


def _open_logs(_path):
    '''
    Quick function to open logs and return a Pandas DataFrame of the job names
    and job ids from the export log.

    Parameters: _path STRING the path to be opened

    Returns: Pandas DataFrame
    '''
    important_logs = []
    log_dict = {}

    with open(_path) as filename:
        f = filename.readlines()
        print("READING LOGS ======")

    for i, line in enumerate(f):
        important_logs.append(json.loads(line))
        log_dict.update({important_logs[i]['path']: int(important_logs[i]['object_id'])})
        #print(important_logs[i]['path'], important_logs[i]['object_id'])

    return pd.DataFrame(data=list(log_dict.items()), columns=['path', 'object_id'])

def main(NEW_PATH, OLD_PATH, OLD_URL, NEW_URL, E2_WORKSPACE_ID):
    # Open the old WS logs
    oldPath = OLD_PATH
    newPath = NEW_PATH

    old_log_df = _open_logs(oldPath)
    old_log_df['path'] = old_log_df['path'].astype(str)
    print(old_log_df.head())


    new_log_df = _open_logs(newPath)
    new_log_df['path'] = new_log_df['path'].astype(str)
    print(new_log_df.head())
    new_log_df['path_replaced_archive_with_users'] = new_log_df['path'].str.replace("Archive", "Users")
    join_df = pd.merge(old_log_df, new_log_df, "left", left_on="path", right_on = "path_replaced_archive_with_users", suffixes=("_old", "_new"))

    #print(join_df.head())

    join_df = join_df[['path_old', 'object_id_old', 'object_id_new']]
    join_df['notebook_url_old'] = f"https://{str(OLD_URL)}/#notebook/" + join_df['object_id_old'].astype(str)
    join_df['notebook_url_new'] = f"https://{str(NEW_URL)}/?o={str(E2_WORKSPACE_ID)}#notebook/" + join_df['object_id_new'].astype(str)

    #print(f"Found {len(old_log_df)} notebooks in ST workspace and {len(new_log_df)} notebooks in E2 workspace.")
    #print("Saving the csv file of all notebook mappings...")
    join_df.to_csv("./all_notebook_mapping.csv")


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Map notebooks between workspaces.")
    parser.add_argument("--newPath", "--NEW", dest="NEW_PATH", help="Path to the new user_workspace.log file.")
    parser.add_argument("--oldPath", "--OLD", dest="OLD_PATH", help="Path to the old user_workspace.log file.")

    parser.add_argument("--oldURL", dest="OLD_URL", help="Workspace URL of ST workspace.")
    parser.add_argument("--newURL", dest="NEW_URL", help="Workspace URL of E2 workspace.")
    parser.add_argument("--workspace-id", dest="E2_WORKSPACE_ID", help="Workspace ID of E2 workspace.")

    parser = parser.parse_args()
    main(parser.NEW_PATH, parser.OLD_PATH, parser.OLD_URL, parser.NEW_URL, parser.E2_WORKSPACE_ID)