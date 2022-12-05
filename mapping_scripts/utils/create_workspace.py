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

        # this is where all assets are mapped to what csv they refer to + what function they use for the split
        self.map = {
            'users': ["users.csv", split.users],
            'instance_pools' : ["instance_pools.csv", split.instance_pools],
            'instance_profiles': ["instance_profiles.csv", split.instance_profiles],
            'groups': ["groups.csv", split.groups],
            'jobs': ["jobs.csv", split.jobs],
            'acl_jobs': ["jobs.csv", split.acl_jobs],
            'secret_scopes': ["secret_scopes.csv", split.secret_scopes],
            'secret_scopes_acls':["secret_scopes.csv", split.secret_scopes_acls],
            'clusters': ["clusters.csv", split.clusters],
            'cluster_policies': ["clusters.csv", split.cluster_policy],
            'acl_clusters':["clusters.csv", split.acl_clusters],
            'acl_cluster_policies': ["clusters.csv", split.acl_cluster_policies],
            'mounts': ["mounts.csv", split.mounts],
            'shared_notebooks': ["global_shared_logs.csv", split.shared_notebooks],
            'global_notebooks': ["global_logs.csv", split.global_notebooks],
            'user_notebooks': ["users.csv", split.user_notebooks],
            'user_dirs': ["users.csv", split.user_dirs],
            'user_workspace': ["users.csv", split.user_workspace],
            'acl_notebooks':["users.csv", split.acl_notebooks],
            'acl_directories':["users.csv", split.acl_directories],
            'metastore': ["metastore.csv", split.metastore],
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

    def copy_other_files(self, workspace_skipped_csv_dict):
        """
        summary: copy files that need to be copied to all workspace folders
        """
        total = ['app_logs', 'checkpoint', 'database_details.log', 'source_info.txt']
        for w in self.workspaces:
            # don't copy the logs that were not in the csvs directory
            skipped_csvs = workspace_skipped_csv_dict[w]
            total_in_workspace = os.listdir("./logs/"+self.checkpoint+"_"+w)
            for file in total:
                if file not in self.workspaces:
                    try:
                        # if it is a file, copy just that file. otherwise, copy all files recursively in it
                        if os.path.isfile("./logs/"+self.checkpoint+"/"+file):
                            #print(f"Copying file {file} to workspace {w}")
                            shutil.copy("./logs/"+self.checkpoint+"/"+file, "./logs/"+self.checkpoint+"_"+w+"/"+file)
                        else:
                            #print(f"Copying directory {file} to workspace {w}")
                            shutil.copytree("./logs/"+self.checkpoint+"/"+file, "./logs/"+self.checkpoint+"_"+w+"/"+file)
                    except Exception as e:
                        pass

    def run(self):
        """
        summary: run each module for every asset
        """
        # for each
        for m in self.map.keys():
            print(f"    Starting with {m}...")
            try:
                # get the asset function that splits that asset
                module_function = self.map[m][1]
                # get the appropriate csv that matches it
                csv = self.map[m][0]
                # split_csv performs the actual split and outputs all csvs that were not in the csv directory
                skipped_csv = self.split_csv(m, module_function, csv)
            except Exception as e:
                #print(f"{self.map[m][0]} was not found. ")
                #print(e)
                pass
        return skipped_csv

    def split_csv(self, module, module_function, csv):
        skipped_csv = []
        if csv not in os.listdir("./csv"):
            #print(f"{csv} not found. Skipping...")
            skipped_csv.append(csv)
            return 1
        # reads csv and inputs attribute columns where the workspace column is set to Y
        # you can set that variable to True or 1 or anything else that the client is using
        # but it will ignore anything else
        df = pd.read_csv("./csv/"+csv, index_col=0)
        current_df = df[df[self.workspace] == "Y"]
        # send that subset dataframe to the module function found in Split class
        success = module_function(current_df.reset_index())
        # success should be 0
        return skipped_csv
