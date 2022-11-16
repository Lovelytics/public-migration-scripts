import json
import os
import shutil
import pandas as pd
import gzip

class Split():
    def __init__(self, checkpoint, workspace):
        self.path = "./logs/"+checkpoint+"/"
        self.workspace = workspace
        self.new_path = "./logs/"+checkpoint+"_"+workspace+"/"
        self.not_imported_users = []
        self.not_imported_groups = []

    def read_log(self, file_name):
        """
        summary: reads a given log
        """
        try:
            with open(self.path+file_name) as f:
                data = f.read().split("\n")
            return data
        except FileNotFoundError as e:
            return print("File not found. ")
        except Exception as e:
            print(f"There was an error while reading {file_name}. ")
            print(e)
            return ''

    def users(self, df, file_name="users.log"):
        self.not_imported_users = []
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if d['userName'] in df['userName'].tolist():
                        data_write.append(d)
                    else:
                        if d['userName'] not in df['userName'].tolist():
                            #print(f"{d['userName']} not in the workspace. Skipping..")
                            self.not_imported_users.append(d['userName'])
            except Exception as e:
                print(f"There was an error splitting {file_name}")
                print(e)
                #print("ERROR in splitting users.log")
        return data_write

    def user_dirs(self, df=None, file_name="user_dirs.log"):
        data_user = df
        user_names = data_user['userName'].tolist()
        if "global_shared_logs" in os.listdir("./csv/"):
            data_art = pd.read_csv('./csv/global_shared_logs.csv', index_col=0)
            art_names = data_art['global_shared_folder_names'].tolist()
        else:
            data_art = []
            art_names = []
        if "shared_logs" in os.listdir("./csv/"):
            data_shared = pd.read_csv('./csv/shared_logs.csv', index_col=0)
            shared_names = data_shared['notebook_names'].tolist()
        else:
            data_shared = []
            shared_names = []
        data = self.read_log(file_name)
        user_paths=['/Users/'+ n for n in user_names]
        shared_paths=['/Shared/'+ n for n in shared_names]
        data_write = []
        for d in data:
            if d != '':
                try:
                    d = json.loads(d)
                    path = str(d['path'])
                    if (path[1:].startswith(tuple(art_names)) or path.startswith(tuple(user_paths)) or path.startswith(tuple(shared_paths))):
                        data_write.append(d)
                except Exception as e:
                    print(f"There was an error splitting {file_name}")
                    print(e)
        return data_write

    def user_workspace(self, df, file_name="user_workspace.log"):
        data_user = df
        user_names = data_user['userName'].tolist()

        if "global_shared_logs" in os.listdir("./csv/"):
            data_art = pd.read_csv('csv/global_shared_logs.csv', index_col=0)
            art_names = data_art['global_shared_folder_names'].tolist()
        else:
            data_art = []
            art_names = []
        if "shared_logs" in os.listdir("./csv/"):
            data_shared = pd.read_csv('csv/shared_logs.csv', index_col=0)
            shared_names = data_shared['notebook_names'].tolist()
        else:
            data_shared = []
            shared_names = []
        data = self.read_log(file_name)
        user_paths=['/Users/'+ n for n in user_names]
        shared_paths=['/Shared/'+ n for n in shared_names]
        data_write = []
        for d in data:
            if d != '':
                try:
                    d = json.loads(d)
                    path = str(d['path'])
                    if (path[1:].startswith(tuple(art_names)) or path.startswith(tuple(user_paths)) or path.startswith(tuple(shared_paths))):
                        data_write.append(d)
                except Exception as e:
                    print(f"There was an error splitting {file_name}")
        return data_write

    def instance_pools(self, df, file_name="instance_pools.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[df['instance_pool_id'] == d['instance_pool_id']]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")

        return data_write

    def libraries(self, df, file_name="libraries.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['library_paths'] == d['path'])]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def jobs(self, df, file_name="jobs.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['job_ids'] == d['job_id'])]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def acl_jobs(self, df, file_name="acl_jobs.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    jobid = d['object_id'].split("/")[-1]
                    if len(df.loc[df['job_ids'] == int(jobid)]) > 0:
                        data_write.append(d)
                    if "access_control_list" in d.keys():
                        d['access_control_list'] = self.fix_acls(d['access_control_list'])
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def secret_scopes(self, df, file_name=None):
        scopes = df["secret_scope_names"]
        for scope in scopes:
            if "secret_scopes" not in os.listdir(self.new_path):
                os.mkdir(self.new_path+"secret_scopes")
            new_file_path = self.new_path+"secret_scopes/"+scope
            src_path = self.path+"secret_scopes/"+scope
            shutil.copyfile(src_path,new_file_path)
        return 0

    def secret_scopes_acls(self, df, file_name="secret_scopes_acls.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['secret_scope_names'] == d['scope_name'])]) > 0:
                        data_write.append(d)
                    if "items" in d.keys():
                        d['items'] = self.fix_acls(d['items'])
            except Exception as e:
                print(f"There was an error splitting {file_name}")
                print(e)
        return data_write

    def clusters(self, df, file_name = "clusters.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['cluster_name'] == d['cluster_name'])]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def cluster_policy(self, df, file_name = "cluster_policies.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['policy_id'] == d['policy_id'])]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def acl_clusters(self, df, file_name = "acl_clusters.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    cluster = d['object_id'].split("/")[-1]
                    if cluster in df['cluster_id'].tolist():
                        data_write.append(d)
                    if "access_control_list" in d.keys():
                        d['access_control_list'] = self.fix_acls(d['access_control_list'])
            except Exception as e:
                print(f"There was an error splitting {file_name}")
                print(e)
        return data_write

    def acl_cluster_policies(self, df, file_name = "acl_cluster_policies.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    policy = d['object_id'].split("/")[-1]
                    if len(df.loc[(df['policy_id'] == policy)]) > 0:
                        data_write.append(d)
                    if "access_control_list" in d.keys():
                        d['access_control_list'] = self.fix_acls(d['access_control_list'])
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def instance_profiles(self, df, file_name="instance_profiles.log"):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['instance_profile_arn'] == d['instance_profile_arn'])]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
                print(e)
        return data_write

    def mounts(self, df, file_name='mounts.log'):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['mount_paths'] == d['path'])]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def groups(self, df, file_name=None):
        groups = df['group_name']
        for group in groups:
            try:
                if "groups" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path + "groups/")
                new_file_path = self.new_path + "groups/" + group
                src_path = self.path + "groups/" + group
                shutil.copy(src_path,new_file_path)
            except:
                print(f"There was an error splitting {file_name}")

        all_groups = os.listdir(self.path + "groups")
        self.not_imported_groups = [g for g in all_groups if g not in groups ]
        return 0

    def shared_logs(self, df, file_name=None):
        names = df['notebook_names']
        for notebook in names:
            try:
                if "Shared" not in os.listdir(self.new_path+"artifacts/Shared/"):
                    os.mkdir(self.new_path+'artifacts/Shared/')
                new_folder_path = self.new_path+'artifacts/Shared/'+notebook
                src_path = self.path+'artifacts/Shared/'+notebook
                shutil.copytree(src_path,new_folder_path)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return 0

    def metastore(self, df, file_name=None):
        databases = os.listdir(self.path + "metastore/")
        for db in df['metastore_database']:
            try:
                if "metastore" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path+"metastore/")

                new_folder_path = self.new_path+"metastore/"+db
                src_path = self.path+"metastore/"+db
                if db not in os.listdir(self.new_path+"metastore/"):
                    shutil.copytree(src_path, new_folder_path)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return 0

    def success_metastore(self, df, file_name='success_metastore.log'):
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    database = d['table'].split(".")[0]
                    if len(df.loc[(df['metastore_database'] == database)]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")
        return data_write

    def artifacts(self, df, file_name=None):
        data_user = df
        artifacts = os.listdir(self.path+"artifacts")
        if "global_shared_logs" in os.listdir("./csv/"):
            data_art = pd.read_csv('./csv/global_shared_logs.csv', index_col=0)
            art_names = data_art['global_shared_folder_names'].tolist()
            art = True
        else:
            data_art = []
            art_names = []
            art = False
        if "shared_logs" in os.listdir("./csv/"):
            data_shared = pd.read_csv('./csv/shared_logs.csv', index_col=0)
            shared_names = data_shared['notebook_names'].tolist()
            share = True
        else:
            data_shared = []
            shared_names = []
            share = False
        if art:
            for a in data_art['global_shared_folder_names']:
                if "artifacts" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path+"artifacts")
                else:
                    new_folder_path = self.new_path+"artifacts/"+a
                    src_path = self.path+"artifacts/"+a
                    if a not in os.listdir(self.new_path+"artifacts/"):
                        if os.path.isdir(src_path):
                            shutil.copytree(src_path, new_folder_path)
                        else:
                            shutil.copy(src_path, new_folder_path)
        if share:
            if "Shared" in artifacts:
                for s in data_shared['notebook_names']:
                    if s in os.listdir(self.path+"artifacts/Shared"):
                        if "Shared" not in os.listdir(self.new_path+"artifacts"):
                            os.mkdir(self.new_path+"artifacts/Shared")
                        else:
                            new_folder_path = self.new_path+"artifacts/Shared/"+u
                            src_path = self.path+"artifacts/Shared/"+u
                            if u not in os.listdir(self.new_path+"artifacts/Shared/"):
                                shutil.copytree(src_path, new_folder_path)
        if "Users" in artifacts:
            for u in data_user['userName']:
                if u in os.listdir(self.path+"artifacts/Users"):
                    if "artifacts" not in os.listdir(self.new_path):
                        os.mkdir(self.new_path+"artifacts")
                    if "Users" not in os.listdir(self.new_path+"artifacts"):
                        os.mkdir(self.new_path+"artifacts/Users")
                    else:
                        new_folder_path = self.new_path+"artifacts/Users/"+u
                        src_path = self.path+"artifacts/Users/"+u
                        if u not in os.listdir(self.new_path+"artifacts/Users/"):
                            shutil.copytree(src_path, new_folder_path)

        return 0

    def acl_notebooks(self, df, file_name="acl_notebooks.log"):
        data_user = df
        user_names = data_user['userName'].tolist()
        if "global_shared_logs" in os.listdir("./csv/"):
            data_art = pd.read_csv('csv/global_shared_logs.csv', index_col=0)
            art_names = data_art['global_shared_folder_names'].tolist()
        else:
            data_art = []
            art_names = []
        if "shared_logs" in os.listdir("./csv/"):
            data_shared = pd.read_csv('csv/shared_logs.csv', index_col=0)
            shared_names = data_shared['notebook_names'].tolist()
        else:
            data_shared = []
            shared_names = []
        data = self.read_log(file_name)
        user_paths=['/Users/'+ n for n in user_names]
        shared_paths=['/Shared/'+ n for n in shared_names]
        data_write = []
        for d in data:
            if d != '':
                try:
                    d = json.loads(d)
                    path = str(d['path'])
                    if (path[1:].startswith(tuple(art_names)) or path.startswith(tuple(user_paths)) or path.startswith(tuple(shared_paths))):
                        data_write.append(d)
                    if "access_control_list" in d.keys():
                        d['access_control_list'] = self.fix_acls(d['access_control_list'])
                except Exception as e:
                    print(f"There was an error splitting {file_name}")
        return data_write

    def acl_directories(self, df, file_name="acl_directories.log"):
        data_user = df
        user_names = data_user['userName'].tolist()
        if "global_shared_logs" in os.listdir("./csv/"):
            data_art = pd.read_csv('csv/global_shared_logs.csv', index_col=0)
            art_names = data_art['global_shared_folder_names'].tolist()
        else:
            data_art = []
            art_names = []
        if "shared_logs" in os.listdir("./csv/"):
            data_shared = pd.read_csv('csv/shared_logs.csv', index_col=0)
            shared_names = data_shared['notebook_names'].tolist()
        else:
            data_shared = []
            shared_names = []
        data = self.read_log(file_name)
        user_paths=['/Users/'+ n for n in user_names]
        shared_paths=['/Shared/'+ n for n in shared_names]
        data_write = []
        for d in data:
            if d != '':
                try:
                    d = json.loads(d)
                    path = str(d['path'])
                    if (path[1:].startswith(tuple(art_names)) or path.startswith(tuple(user_paths)) or path.startswith(tuple(shared_paths))):
                        data_write.append(d)
                    if "access_control_list" in d.keys():
                        d['access_control_list'] = self.fix_acls(d['access_control_list'])
                except Exception as e:
                    print(f"There was an error splitting {file_name}")
        return data_write

    def table_acls(self, df, file_name="logs/table_acls/00_table_acls.json.gz"):
        with gzip.open(file_name, 'rb') as f_in:
            with open(self.path+"table_acls/00_table_acls.json", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        data = self.read_log('table_acls/00_table_acls.json')
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if len(df.loc[(df['metastore_database'] == d['Database'])]) > 0:
                        data_write.append(d)
            except Exception as e:
                print(f"There was an error splitting {file_name}")

        if "table_acls" not in os.listdir(self.new_path):
            os.mkdir(self.new_path+"table_acls")
        file_path = self.new_path+"table_acls/00_table_acls.json"
        with open(file_path, 'w') as f:
            json.dump(data_write, f)
        return 0

    def fix_acls(self, acls):

        not_imported_users = self.not_imported_users
        not_imported_groups = self.not_imported_groups
        for permission in acls:
            if 'group_name' in permission.keys():
                if permission['group_name'] in not_imported_groups:
                    acls.remove(permission)
            if 'user_name' in permission.keys():
                if permission['user_name'] in not_imported_users:
                    acls.remove(permission)
            if 'principal' in permission.keys():
                if permission['principal'] in not_imported_users:
                    acls.remove(permission)

        return acls
