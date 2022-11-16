from utils.split_logs import Split
import os
import json
import shutil
import pandas as pd

class Workspace():
    def __init__(self, checkpoint, workspace, all_workspaces):
        self.path = "./logs/"+checkpoint+"/"
        self.workspace = str(workspace)
        self.new_path = "./logs/"+checkpoint+"_"+workspace+"/"
        self.workspaces = all_workspaces
        self.checkpoint = checkpoint
        split = Split(checkpoint, workspace)

        self.map = {
            'users': ["users.csv", split.users],
            'instance_pools' : ["instance_pools.csv", split.instance_pools],
            'libraries': ["libraries.csv", split.libraries],
            'jobs': ["jobs.csv", split.jobs],
            'secret_scopes': ["secret_scopes.csv", split.secret_scopes],
            'clusters': ["clusters.csv", split.clusters],
            'instance_profiles': ["instance_profiles.csv", split.instance_profiles],
            'mounts': ["mounts.csv", split.mounts],
            'groups': ["groups.csv", split.groups],
            'shared_logs': ["shared_logs.csv", split.shared_logs],
            'cluster_policies': ["clusters.csv", split.cluster_policy],
            'acl_cluster_policies': ["clusters.csv", split.acl_cluster_policies],
            'acl_clusters':["clusters.csv", split.acl_clusters],
            'secret_scopes_acls':["secret_scopes.csv", split.secret_scopes_acls],
            'acl_jobs': ["jobs.csv", split.acl_jobs],
            'user_workspace': ["users.csv", split.user_workspace],
            'user_dirs': ["users.csv", split.user_dirs],
            'metastore': ["metastore.csv", split.metastore],
            'artifacts': ["users.csv", split.artifacts],
            'acl_notebooks':["users.csv", split.acl_notebooks],
            'acl_directories':["users.csv", split.acl_directories],
            'success_metastore': ["metastore.csv", split.success_metastore],
            'table_acls':["metastore.csv", split.table_acls]
        }
        print("*"*80)
        print(f"Starting with workspace {workspace}...")
        self.create_workspace(workspace, checkpoint)

    @staticmethod
    def create_workspace(wk="test", checkpoint=""):
        """
        summary: creates a directory for each workspace
        """
        directories = os.listdir("./logs/")
        name = checkpoint+"_"+wk
        if name not in directories:
            os.mkdir("./logs/"+name)
            #print("Workspace directory {} was successfully created.".format(name))

    @staticmethod
    def write_logs(log, path, file_name):
        file_path = path+file_name
        #print(file_path)
        with open(file_path, 'w') as f:
            for l in log:
                f.write(json.dumps(l) + '\n')

    def copy_other_files(self, workspace_skipped_csv_dict):
        total = ['app_logs', 'checkpoint', 'database_details.log', 'source_info.txt']
        for w in self.workspaces:
            skipped_csvs = workspace_skipped_csv_dict[w]
            total_in_workspace = os.listdir("./logs/"+self.checkpoint+"_"+w)
            for file in total:
                if file not in self.workspaces:
                    try:
                        if os.path.isfile("./logs/"+file):
                            #print(f"Copying file {file} to workspace {w}")
                            shutil.copy("./logs/"+self.checkpoint+"/"+file, "./logs/"+self.checkpoint+"_"+w+"/"+file)
                        else:
                            #print(f"Copying directory {file} to workspace {w}")
                            shutil.copytree("./logs/"+self.checkpoint+"/"+file, "./logs/"+self.checkpoint+"_"+w+"/"+file)
                    except Exception as e:
                        pass

    def run(self):
        for m in self.map.keys():
            #print(f"Starting with {m}...")
            try:
                module_function = self.map[m][1]
                csv = self.map[m][0]
                skipped_csv = self.split_csv(m, module_function, csv)
            except Exception as e:
                #print(f"{self.map[m][0]} was not found. ")
                #print(e)
                pass
        return skipped_csv

    def split_csv(self, module, module_function, csv):
        skipped_csv = []
        if csv not in os.listdir("./csv"):
            print(f"{csv} not found. Skipping...")
            skipped_csv.append(csv)
            return 1

        df = pd.read_csv("./csv/"+csv, index_col=0)
        current_df = df[df[self.workspace] == "Y"]
        logs = module_function(current_df.reset_index())

        if logs != 0:
            #print(f"Writing split {module} logs for workspace {self.workspace}")
            self.write_logs(logs, self.new_path, module+".log")
        return skipped_csv
