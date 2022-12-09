import argparse
import json
import time
import requests
import pandas as pd

def _get_jobs_list_api_call(workspaceURLST, tokenST, limit, offset):
    '''
        Makes a GET request to the Databricks Jobs API to retrieve a list of
        jobs from a workspace. Number of jobs returned is limited by the API.

        Parameters:
        - workspaceURLST (STRING): Workspace URL
        - tokenST (STRING): Workspace access token
        - limit (INT): Number of jobs to return. Must be > 0 and <= 25. Default is 20
        - offset (INT): Offset of the first job to return, relative to the most recently created job
    '''

    requestsURL = workspaceURLST + f"/api/2.1/jobs/list?limit={limit}&offset={offset}"
    headers = {
        'Authorization': f'Bearer {tokenST}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)

    job_list = []

    if response.status_code == 200:
        jobListJSON = response.json()

        try:
            for job in jobListJSON['jobs']:
                job_list.append(str(job['job_id']))

        except KeyError:
            pass

    else:
        print(response.text)
    
    return job_list


def _get_full_jobs_list(workspaceURLST, tokenST):
    '''
        Makes multiple GET requests to the Databricks Jobs API to retrieve the full list of
        jobs from a workspace.

        Parameters:
        - workspaceURLST (STRING): Workspace URL
        - tokenST (STRING): Workspace access token
    '''

    print(f"Getting list of jobs from workspace {workspaceURLST}...")
    limit = 25
    responseJobCount = limit
    offset = 0
    
    job_list = []

    while responseJobCount == limit:
        responseJobs = _get_jobs_list_api_call(workspaceURLST, tokenST, limit, offset)
        responseJobCount = len(responseJobs)
        job_list.extend(responseJobs)
        if responseJobCount == limit:
            offset += limit
        time.sleep(0.5)

    return job_list


def _get_cluster_policies(workspaceURL, token):

    print(f"Getting cluster policies from {workspaceURL}...")

    url = workspaceURL + '/' if workspaceURL[-1] != '/' else workspaceURL

    url += "api/2.0/policies/clusters/list"

    payload={}
    headers = {
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    try:
        if response.status_code == 200 and 'policies' in list(response.json().keys()):
            cluster_policies = response.json()['policies']
            print(f"Successfully found {len(cluster_policies)} policies in {workspaceURL}.")
            return cluster_policies
        else:
            print(response.text)
            return []
    except Exception as e:
        print(e)
        print("Moving on...")
        return []

def _replace_cluster_policy_id(E2URL, tokenE2, jobJSON, STURL, tokenST):
    E2_cluster_policies = _get_cluster_policies(E2URL, tokenE2)
    ST_cluster_policies = _get_cluster_policies(STURL, tokenST)
    try:
        for i, _ in enumerate(jobJSON['job_clusters']):
            if 'new_cluster' in list(jobJSON['job_clusters'][i].keys()):
                job_cluster_policy_id = jobJSON['job_clusters'][i]['new_cluster']['policy_id']
                print(f"Found cluster policy associated with job... {job_cluster_policy_id}...")
                cluster_policy_name = [policy for policy in ST_cluster_policies if policy['policy_id'] == job_cluster_policy_id]
                if len(cluster_policy_name) > 0:
                    cluster_policy_name = cluster_policy_name[0]
                    new_policy = [policy for policy in E2_cluster_policies if policy['name'] == cluster_policy_name['name']]
                    if len(new_policy) > 0:
                        jobJSON['job_clusters'][i]['new_cluster']['policy_id'] = new_policy[0]['policy_id']
                        print(f"Successfully changed the policy id from {cluster_policy_name['policy_id']} to {new_policy[0]['policy_id']}...")
                        return jobJSON
                    else:
                        print("Couldn't find the policy name within the E2 cluster policies... debugging output")
                        print(cluster_policy_name)
                        print(E2_cluster_policies)
                        return jobJSON
                else:
                    print("Couldn't find the id within the ST cluster policies... debugging output")
                    print(job_cluster_policy_id)
                    print(ST_cluster_policies)
                    return jobJSON
    except KeyError as e:
        print("No policy_id found under settings > job_clusters for the job json configuration file.")
        return jobJSON
        

def _get_job_api_call(workspaceURLST, jobID, tokenST):
    print(f"Getting job {jobID}...")
    requestsURL = workspaceURLST + "/api/2.1/jobs/get?job_id="
    requestsURL += jobID
    headers = {
        'Authorization': f'Bearer {tokenST}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)

    if response.status_code == 200:
        jobJSON = response.json()
        print(jobJSON)
        jobSettings = jobJSON['settings']
        print(jobSettings)
        if "job_clusters" in list(jobSettings.keys()):
            print(f"Successfully called job {jobID}. Name: {jobSettings['name']}. Format: {jobSettings['format']}. Tasks: {len(jobSettings['tasks'])}. Clusters: {len(jobSettings['job_clusters'])}.")
        else:
            print(f"Successfully called job {jobID}. Name: {jobSettings['name']}. Format: {jobSettings['format']}. Tasks: {len(jobSettings['tasks'])}.")
        return jobSettings
    else:
        print(response.text)
        return None

def _get_instance_pool_name(STURL, STTOKEN, IPid):
    print(f"Getting instance pool name for instance profile ID {IPid}...")
    requestsURL = STURL + "/api/2.0/instance-pools/get?instance_pool_id="
    requestsURL += IPid
    headers = {
        'Authorization': f'Bearer {STTOKEN}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)
    if response.status_code == 200:
        instance_profile_name = response.json()['instance_pool_name']
        print(f"Found name: {instance_profile_name}")
        return instance_profile_name
    else:
        print(response.text)
        return "Failed"

def _get_instance_pool_list(E2URL, E2TOKEN):
    print(f"Getting all instance profiles in {E2URL}...")
    requestsURL = E2URL + "/api/2.0/instance-pools/list"
    headers = {
        'Authorization': f'Bearer {E2TOKEN}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)
    if response.status_code == 200:
        try:
            all_instance_profiles = response.json()["instance_pools"]
            print(f"Found {len(all_instance_profiles)} instance pools")
        except KeyError:
            print(f"No instance pools found in {E2URL}")
            all_instance_profiles = None
        return all_instance_profiles
    else:
        print(response.text)
        return "Failed"

def _replace_instance_profile_ARN(arn_mapping, jobJSON):
    try:
        for task in jobJSON['tasks']:
            try:
                old_ip_arn = task['new_cluster']['aws_attributes']['instance_profile_arn']
                task['new_cluster']['aws_attributes']['instance_profile_arn'] = arn_mapping[old_ip_arn]
            except KeyError:
                print("No instance profile ARN for task found.")
    except KeyError:
        print("No tasks found.")
    return jobJSON
        

def _replace_instance_pools(E2URL, tokenE2, jobJSON, STURL, tokenST):
    allE2IPs = _get_instance_pool_list(E2URL, tokenE2)
    for i, task in enumerate(jobJSON["tasks"]):
        if "new_cluster" in list(task.keys()):
            print("Found new cluster...")
            print(task["new_cluster"].keys())
            if "instance_pool_id" in list(task["new_cluster"].keys()):
                IPid = task['new_cluster']['instance_pool_id']
                IPname = _get_instance_pool_name(STURL, tokenST, IPid)
                print(f"Found instance pool {IPname}...")
                print(allE2IPs)
                if allE2IPs is None:
                    print("Script failed due to a lack of instance pools imported into the E2 environment.")
                    print(IPname)
                for E2IP in allE2IPs:
                    if E2IP["instance_pool_name"] == IPname:
                        newID = E2IP["instance_pool_id"]
                        print(f"Replacing instance pool id {IPid} with id {newID}")
                        jobJSON["tasks"][i]["new_cluster"]["instance_pool_id"] = newID
    print(f"Completed instance pool switch")
    return jobJSON


def _get_cluster_name(STURL, STTOKEN, clusterID):
    print(f"Getting cluster name for instance profile ID {clusterID}...")
    requestsURL = STURL + "/api/2.0/clusters/get?cluster_id="
    requestsURL += clusterID
    headers = {
        'Authorization': f'Bearer {STTOKEN}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)
    if response.status_code == 200:
        cluster_name = response.json()['cluster_name']
        print(f"Found name: {cluster_name}")
        return cluster_name
    else:
        print(response.text)
        return "Failed"

def _get_cluster_list(E2URL, E2TOKEN):
    print(f"Getting all clusters in {E2URL}...")
    requestsURL = E2URL + "/api/2.0/clusters/list"
    headers = {
        'Authorization': f'Bearer {E2TOKEN}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)
    if response.status_code == 200:
        all_clusters = response.json()["clusters"]
        print(f"Found {len(all_clusters)} clusters")
        return all_clusters
    else:
        print(response.text)
        return "Failed"

def _replace_cluster_id(E2URL, tokenE2, jobJSON, STURL, tokenST):
    all_clusters = _get_cluster_list(E2URL, tokenE2)
    for i, task in enumerate(jobJSON["tasks"]):
        if "existing_cluster_id" in list(task.keys()):
            print("Found new cluster...")
            IPid = task['existing_cluster_id']
            IPname = _get_cluster_name(STURL, tokenST, IPid)
            for E2IP in all_clusters:
                if E2IP["cluster_name"] == IPname:
                    newID = E2IP["cluster_id"]
                    print(f"Replacing cluster id {IPid} with id {newID}")
                    jobJSON["tasks"][i]["existing_cluster_id"] = newID
    print(f"Completed cluster switch")
    return jobJSON

def _post_job_api_call(workspaceURLE2, tokenE2, jobJSON):
    print("Starting POST call to create the job...")
    requestsURL = workspaceURLE2 + "/api/2.1/jobs/create"
    try:
        jobJSON['schedule']['pause_status'] = 'PAUSED'
    except KeyError:
        print("No schedule found... skipping PAUSED state. Please check E2 job to ensure it is paused.")
    payload = json.dumps(jobJSON)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {tokenE2}'
    }
    print(requestsURL)
    response = requests.request("POST", requestsURL, headers=headers, data=payload)

    if response.status_code == 200:
        newJobID = response.json()['job_id']
        print(f"Successfully created job {jobJSON['name']}. New ID: {newJobID}.")
        return newJobID
    else:
        print(response.text)
        return 'Failed'

def _get_job_permissions_api_call(workspaceURLST, jobID, tokenST):
    print(f"Getting job {jobID}...")
    requestsURL = workspaceURLST + "/api/2.0/permissions/jobs/"
    requestsURL += str(jobID)
    headers = {
        'Authorization': f'Bearer {tokenST}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)

    if response.status_code == 200:
        jobJSON = response.json()
        jobPermissions = jobJSON['access_control_list']
        print(jobPermissions)
        print(f"Successfully called job {jobID}. ACLs configured: {len(jobPermissions)}.")
        return jobPermissions
    else:
        print(response.text)
        return None

def _get_E2_user_list(workspaceURLE2, tokenE2):
    print(f"Handling user capitalization issue... getting E2 users.")
    requestsURL = workspaceURLE2 + "/api/2.0/preview/scim/v2/Users"
    headers = {
        'Authorization': f'Bearer {tokenE2}'
    }
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers)
    e2_user_list = []
    if response.status_code == 200:
        responseJSON = response.json()
        try:
            for resource in responseJSON['Resources']:
                for email in resource['emails']:
                    e2_user_list.append(email['value'])
        except Exception as e:
            print(e)
        return e2_user_list
    else:
        print(response.text)
        return e2_user_list


def _handle_username_capitalization(E2, tokenE2, jobJSON, newJobID):
    e2_user_list = _get_E2_user_list(E2, tokenE2)
    allPermissions = []

    permissionLevels = [[perm['permission_level'] for perm in permissionACL['all_permissions']] for permissionACL in jobJSON]
    if "IS_OWNER" not in permissionLevels:
        addOwner = True

    for permissionACL in jobJSON:
        print(permissionACL)
        permissionACL['permission_level'] = permissionACL['all_permissions'][0]['permission_level']
        del permissionACL['all_permissions']
        if "user_name" in permissionACL.keys():
            old_user = permissionACL['user_name']
            new_user = [user for user in e2_user_list if user.lower() == old_user.lower()][0]
            print(f'Changing username {old_user} to {new_user}.')
            permissionACL['user_name'] = new_user
        print(permissionACL)
        allPermissions.append(permissionACL)

    aclList = {}

    if addOwner:
        method = 'PUT'
    else:
        method = 'PATCH'

    aclList["access_control_list"] = allPermissions
    requestsURL = E2 + "/api/2.0/permissions/jobs/"
    requestsURL += str(newJobID)
    payload = json.dumps(aclList)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {tokenE2}'
    }
    print(requestsURL)
    response = requests.request(method, requestsURL, headers=headers, data=payload)

    if response.status_code == 200:
        newJobID = response.json()['object_id']
        print(f"Successfully created job permissions {newJobID}.")
        return newJobID
    else:
        print(response.text)
        return 'Failed'


def _post_job_permissions_api_call(workspaceURLE2, tokenE2, jobJSON, newJobID):
    print("Starting POST call to create the job...")
    allPermissions = []
    aclList = {}
    addOwner = False

    permissionLevels = [[perm['permission_level'] for perm in permissionACL['all_permissions']] for permissionACL in jobJSON]
    if "IS_OWNER" not in permissionLevels:
        addOwner = True

    print(jobJSON)
    for permissionACL in jobJSON:
        print(permissionACL)
        permissionACL['permission_level'] = permissionACL['all_permissions'][0]['permission_level']
        del permissionACL['all_permissions']
        print(permissionACL)
        allPermissions.append(permissionACL)

    if addOwner:
        method = 'PUT'
    else:
        method = 'PATCH'

    aclList["access_control_list"] = allPermissions
    requestsURL = workspaceURLE2 + "/api/2.0/permissions/jobs/"
    requestsURL += str(newJobID)
    payload = json.dumps(aclList)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {tokenE2}'
    }
    print(requestsURL)
    response = requests.request(method, requestsURL, headers=headers, data=payload)

    if response.status_code == 200:
        newJobID = response.json()['object_id']
        print(f"Successfully created job permissions {newJobID}.")
        return newJobID
    else:
        print(response.text)
        return 'Failed'

def _job_id_mapper(oldJobID, newjobID, jobName):
    x = '{name:' + jobName + ', oldJobID:' + oldJobID + 'newJobID' + newjobID
    y = json.loads(x)
    return y

def _post_create_all_new_permissions_api_call(E2, E2TOKEN, NEW_OWNER, NEWJOB):

    newACL = {
        "access_control_list": [
            {
                "user_name": "robb.fournier@sportsbet.com.au",
                "permission_type": "CAN_MANAGE"
            },
            {
                "user_name": NEW_OWNER,
                "permission_type": "IS_OWNER"
            }, 
            {
                "group_name": "admins",
                "permission_type": "CAN MANAGE"
            }
        ]
    }

    requestsURL = E2 + "/api/2.0/permissions/jobs/"
    requestsURL += str(NEWJOB)
    payload = json.dumps(newACL)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {E2TOKEN}'
    }
    print(requestsURL)
    response = requests.request("PUT", requestsURL, headers=headers, data=payload)

    if response.status_code == 200:
        newJobID = response.json()['object_id']
        print(f"Successfully added job permissions {newJobID}.")
        return newJobID
    else:
        print(response.text)
        return 'Failed'

    
def main(ST, E2, JOBS, STTOKEN, E2TOKEN, PERMISSIONS_ONLY, NEWJOBS, REPLACE_IP_ARN, OLD_IP_ARNS, NEW_IP_ARNS, NEW_OWNER):
    print("Starting...")

    confirmation1 = input(f"Please confirm ST workspace: {ST}. [YES to continue] :")
    confirmation2 = input(f"Please confirm E2 workspace: {E2}. [YES to continue] :")

    # Check if Job IDs are specified, else migrate all jobs
    if JOBS:
        JOBS = JOBS.split(",")
    else:
        print("No Job IDs specified. All jobs will be migrated.")
        time.sleep(1)
        JOBS = _get_full_jobs_list(ST, STTOKEN)

    confirmation3 = input(f"Please confirm there are {len(JOBS)} jobs to migrate. [YES to continue] :")
    assert confirmation1 == 'YES' and confirmation2 == 'YES' and confirmation3 == 'YES'

    oldIds = []
    newIds = []
    names = []
    oldStatuses = []
    jobDict = {}
    permissions = []

    if REPLACE_IP_ARN == True:
        arn_mapping = {}
        OLD_IP_ARNS = OLD_IP_ARNS.split(",")
        NEW_IP_ARNS = NEW_IP_ARNS.split(",")
        assert len(OLD_IP_ARNS) == len(NEW_IP_ARNS), "Number of old instance profile ARNs specified does not match number of new instance profile ARNs."
        for i in range(0, len(OLD_IP_ARNS)):
            arn_mapping[OLD_IP_ARNS[i]] = NEW_IP_ARNS[i]
        print(arn_mapping)

    if PERMISSIONS_ONLY == True:
        NEWJOBS = NEWJOBS.split(",")
        assert NEWJOBS is not None and len(NEWJOBS) == len(JOBS)
        for JOB, NEWJOB in zip(JOBS, NEWJOBS):
            time.sleep(1)
            oldIds.append(JOB)
            permissionsJSON = _get_job_permissions_api_call(ST, JOB, STTOKEN)
            if permissionsJSON is not None:
                objectId = _post_job_permissions_api_call(E2, E2TOKEN, permissionsJSON, NEWJOB)
                if objectId == 'Failed':
                    permissionsJSON = _get_job_permissions_api_call(ST, JOB, STTOKEN)
                    objectId = _handle_username_capitalization(E2, E2TOKEN, permissionsJSON, NEWJOB)
                newIds.append(NEWJOB)
                permissions.append(permissionsJSON)
                print(f"ACLs imported for {objectId}")
            else:
                objectId = _post_create_all_new_permissions_api_call(E2, E2TOKEN, NEW_OWNER, NEWJOB)
                newIds.append(NEWJOB)
                permissions.append(permissionsJSON)
                print(f"ACLs imported for {objectId}")
        
        jobDict.update(
            {
                'single_tenant_job_ids': oldIds,
                'e2_job_ids': newIds,
                "permissions": permissions
            }
        )

        print("Logging the job permissions...")
        df = pd.DataFrame.from_dict(jobDict)
        df.to_csv("job_permissions.csv")
        print("Job Ids mapping saved to job_permissions.csv")
        print("...Finished")
        
    else:
        for JOB in JOBS:
            print("*"*80)
            print(JOB)
            time.sleep(0.5)
            jobSettings = _get_job_api_call(ST, JOB, STTOKEN)
            oldIds.append(JOB)
            names.append(jobSettings['name'])
            try:
                oldStatuses.append(jobSettings['schedule'])
            except KeyError:
                oldStatuses.append("FLAG: No schedule included in JSON. Check E2 job to ensure paused.")
            time.sleep(1)
            if jobSettings is not None:
                if REPLACE_IP_ARN == True:
                    jobSettings = _replace_instance_profile_ARN(arn_mapping, jobSettings)
                jobSettings = _replace_instance_pools(E2, E2TOKEN, jobSettings, ST, STTOKEN)
                jobSettings = _replace_cluster_id(E2, E2TOKEN, jobSettings, ST, STTOKEN)
                jobSettings = _replace_cluster_policy_id(E2, E2TOKEN, jobSettings, ST, STTOKEN)
                newJOB = _post_job_api_call(E2, E2TOKEN, jobSettings)
                newIds.append(newJOB)
                permissionsJSON = _get_job_permissions_api_call(ST, JOB, STTOKEN)
                if permissionsJSON is not None or permissionsJSON != None:
                    print("Creating E2 permissions...")
                    objectId = _post_job_permissions_api_call(E2, E2TOKEN, permissionsJSON, newJOB)
                    if objectId == 'Failed':
                        permissionsJSON = _get_job_permissions_api_call(ST, JOB, STTOKEN)
                        objectId = _handle_username_capitalization(E2, E2TOKEN, permissionsJSON, newJOB)
                    permissions.append(permissionsJSON)
                    print(f"ACLs imported for {objectId}")
                else:
                    print("No ST permissions found, making new ones in E2...")
                    objectId = _post_create_all_new_permissions_api_call(E2, E2TOKEN, NEW_OWNER, newJOB)
                    permissions.append(permissionsJSON)
                    print(f"ACLs imported for {objectId}")
            else:
                print(f"Job {JOB} failed.")
        
        jobDict.update(
            {
                'job_names': names,
                'single_tenant_job_ids': oldIds,
                'e2_job_ids': newIds,
                'single_tenant_schedule': oldStatuses,
                "permissions": permissions
            }
        )

        print("Logging the job ids...")
        df = pd.DataFrame.from_dict(jobDict)
        df.to_csv("job_mappings.csv")
        print("Job Ids mapping saved to job_mappings.csv.")
        
        print("...Finished")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move sample jobs for an E2 Migration.")
    parser.add_argument("--E2workspace", "--E2", dest="E2", help="URL to the E2 workspace")
    parser.add_argument("--E2token", dest="E2TOKEN", help="E2 token for access.")
    parser.add_argument("--STworkspace", "--ST", dest="ST", help="URL to the ST workspace")
    parser.add_argument("--STtoken", dest="STTOKEN", help="ST token for access.")
    parser.add_argument("--jobIDs", "--ID", dest="JOBS", help="Job IDs to be migrated to E2. Example: 71,22,23.")
    parser.add_argument("--PERMISSIONS_ONLY", "--PERMS", dest="PERMISSIONS_ONLY", action="store_true", help="Flag on whether permissions should be only run. Requires --NEWIDS to be passed.")
    parser.add_argument("--NEWJOBS", dest="NEWJOBS", help="Job IDs from E2, in matching order for ST jobs. Used for permission flag. Example: 71,22,23.")
    parser.add_argument("--REPLACE_IP_ARN", dest="REPLACE_IP_ARN", action="store_true", help="Flag on whether instance profile ARNs should be replaced. Requires --OLD_IP_ARNS and --NEW_IP_ARNS to be passed.")
    parser.add_argument("--OLD_IP_ARNS", dest="OLD_IP_ARNS", help="Old Instance Profile ARNs, in matching order for new E2 Instance Profile ARNs. Used for REPLACE_IP_ARN flag.")
    parser.add_argument("--NEW_IP_ARNS", dest="NEW_IP_ARNS", help="New Instance Profile ARNs, in matching order for ST Instance Profile ARNs. Used for REPLACE_IP_ARN flag.")
    parser.add_argument("--NEW_OWNER", dest="NEW_OWNER", help="Specify a new owner for all jobs.")
    parser = parser.parse_args()
    main(parser.ST, parser.E2, parser.JOBS, parser.STTOKEN, parser.E2TOKEN, parser.PERMISSIONS_ONLY, parser.NEWJOBS, parser.REPLACE_IP_ARN, parser.OLD_IP_ARNS, parser.NEW_IP_ARNS, parser.NEW_OWNER)