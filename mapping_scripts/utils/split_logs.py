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
        self.imported_users = []
        self.imported_groups = ['admins', 'Users']

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

    def write_logs(self, log, file_name):
        """
        summary: function to write a dict to a 'json' log in the same way that
        the original logs are written
        """
        file_path = self.new_path+file_name
        with open(file_path, 'w') as f:
            for l in log:
                f.write(json.dumps(l) + '\n')

    def fix_acls(self, acls):
        new_acls = []
        for permission in acls:
            if 'group_name' in permission.keys():
                if permission['group_name'] in self.imported_groups:
                    new_acls.append(permission)
            if 'user_name' in permission.keys():
                if permission['user_name'] in self.imported_users:
                    new_acls.append(permission)
            if 'principal' in permission.keys():
                if permission['principal'] in self.imported_users:
                    new_acls.append(permission)
            if 'display' in permission.keys():
                if permission['display'] in self.imported_users:
                    new_acls.append(permission)

        return new_acls

    def users(self, df, file_name="users.log"):
        self.imported_users = []
        data = self.read_log(file_name)
        data_write = []
        for d in data:
            try:
                if len(d) != 0:
                    d = d.strip()
                    d = json.loads(d)
                    if d['userName'] in df['userName'].tolist():
                        data_write.append(d)
                        self.imported_users.append(d['userName'])
            except Exception as e:
                pass
        self.write_logs(data_write, file_name)
        return 0


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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

    def secret_scopes(self, df, file_name=None):
        scopes = df["secret_scope_names"]
        for scope in scopes:
            try:
                if "secret_scopes" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path+"secret_scopes")
                new_file_path = self.new_path+"secret_scopes/"+scope
                src_path = self.path+"secret_scopes/"+scope
                shutil.copyfile(src_path,new_file_path)
            except Exception as e:
                pass
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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        self.write_logs(data_write, file_name)
        return 0

    def groups(self, df, file_name=None):
        groups = df['group_name']

        for group in groups:
            try:
                if "groups" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path + "groups/")
                new_file_path = self.new_path + "groups/"
                src_path = self.path + "groups/" + group

                group_data = self.read_log("groups/" + group)
                group_data_write = []
                for d in group_data:
                    if len(d) != 0:
                        d = d.strip()
                        d = json.loads(d)
                        if "members" in d.keys():
                            d['members'] = self.fix_acls(d['members'])
                        group_data_write.append(d)
                self.write_logs(group_data_write, "groups/" + group)
            except:
                pass

        all_groups = os.listdir(self.path + "groups")
        self.imported_groups = [g for g in all_groups if g in groups ]
        return 0

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
                    pass
        self.write_logs(data_write, file_name)
        return 0

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
                    pass
        self.write_logs(data_write, file_name)
        return 0

    def shared_notebooks(self, df, file_name=None):
        names = df['notebook_names']
        for notebook in names:
            try:
                if "artifacts" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path+'artifacts')
                if "Shared" not in os.listdir(self.new_path+"artifacts/Shared/"):
                    os.mkdir(self.new_path+'artifacts/Shared/')
                new_folder_path = self.new_path+'artifacts/Shared/'+notebook
                src_path = self.path+'artifacts/Shared/'+notebook
                shutil.copytree(src_path,new_folder_path)
            except Exception as e:
                pass
        return 0

    def global_notebooks(self, df, file_name=None):
        names = df['global_shared_folder_names']
        for notebook in names:
            try:
                if "artifacts" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path+'artifacts')
                new_folder_path = self.new_path+'artifacts/'+notebook
                src_path = self.path+'artifacts/'+notebook
                shutil.copytree(src_path,new_folder_path)
            except Exception as e:
                pass
        return 0

    def user_notebooks(self, df, file_name=None):
        for u in self.imported_users:
            try:
                if "artifacts" not in os.listdir(self.new_path):
                    os.mkdir(self.new_path+'artifacts')
                if "Users" not in os.listdir(self.new_path + "artifacts/"):
                    os.mkdir(self.new_path+'artifacts/Users/')

                new_folder_path = self.new_path+'artifacts/Users/'+u
                src_path = self.path+'artifacts/Users/'+u
                shutil.copytree(src_path,new_folder_path)
            except Exception as e:
                pass
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
                pass
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
                pass
        self.write_logs(data_write, file_name)
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
                    pass
        self.write_logs(data_write, file_name)
        return 0

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
                    pass
        self.write_logs(data_write, file_name)
        return 0

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
                pass
        if "table_acls" not in os.listdir(self.new_path):
            os.mkdir(self.new_path+"table_acls")
        file_path = self.new_path+"table_acls/00_table_acls.json"
        with open(file_path, 'w') as f:
            json.dump(data_write, f)
        return 0
