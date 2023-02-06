import argparse
import requests
import json
import sys
import time
import os
import datetime

class Databricks(object):

    def __init__(self, **kwargs):
        self.host = kwargs['host'] if 'host' in kwargs else None
        self.token = kwargs['token'] if 'token' in kwargs else None

    def progress(self, _cur, _max):
        p = round(100*_cur/_max)
        b = f"Progress: {_cur}/{_max}"
        print(b, end="\r")

    def collect_jobs(self, job_type):
        host = self.host
        token = self.token
        jobs_list = requests.get("{db_url}/api/2.0/jobs/list".format(db_url=host), headers={
            "Authorization": "Bearer {bearer_token}".format(bearer_token=token),
            "Content-Type": "application/json"})
        jobs = jobs_list.json()['jobs']
        job_ids = []
        job_ids = [{'job_id': job['job_id'], 'created_time': job['created_time']} for job in jobs]
        job_ids = sorted(job_ids, key=lambda i: i['job_id'])

        if job_type == 'infoworks':
            from infoworks.core.mongo_utils import mongodb
            infoworks_metadata_db_jobs = {}
            if len(job_ids) == 0:
                return job_ids
            run_detail_docs = mongodb['databricks_run_details'].find({'created_at': {'$gte': datetime.datetime.fromtimestamp(job_ids[0]['created_time']/1000)}})
            for doc in run_detail_docs:
                infoworks_metadata_db_jobs[doc['databricks_run_properties']['job_id']] = doc['created_at'].timestamp()*1000
            infoworks_only_job_ids = []
            for job in job_ids:
                # match on job_id and maximum of 10 min difference in timestamps
                if job['job_id'] in infoworks_metadata_db_jobs.keys() and ((infoworks_metadata_db_jobs[job['job_id']] - job['created_time'])/60000 <= 10):
                    infoworks_only_job_ids.append({'job_id': job['job_id'], 'created_time': job['created_time']})
            job_ids = infoworks_only_job_ids

        return job_ids

    def delete_jobs(self, job_type):
        host = self.host
        token = self.token

        job_ids = self.collect_jobs(job_type)
        output_file = "/tmp/delete_jobs.txt"
        fd = open(output_file,'w')
        print("job_id,status",file=fd)
        job_num = 0
        job_max = len(job_ids)
        for job_id in job_ids:
            job_runs=requests.get("{db_url}/api/2.0/jobs/runs/list?job_id={jobid}&active_only=true".format(db_url=host,jobid=job_id['job_id']),headers={"Authorization":"Bearer {bearer_token}".format(bearer_token=token),"Content-Type": "application/json"})
            if job_runs.status_code == 200 and "runs" in job_runs.json():
                print("Job "+str(job_id['job_id'])+ " is active. So not deleting this job")
                self.progress(job_num,job_max)
                job_num += 1
                continue
            data = {
                "job_id": "{job_id}".format(job_id=job_id['job_id'])
                }
            result = requests.post("{db_url}/api/2.0/jobs/delete".format(db_url=host),headers={"Authorization":"Bearer {bearer_token}".format(bearer_token=token),"Content-Type": "application/json"},json=data)
            print("{job_id},{status}".format(job_id=job_id,status=result.status_code),file=fd)
            self.progress(job_num,job_max)
            job_num += 1
        print("..."*5, end="\r")
        print("Done")
        fd.close()


class Input(object):
    def __init__(self):
        pass

    def get(self):
        parser = argparse.ArgumentParser(description='Delete databricks Jobs')
        parser.add_argument('-s', '--host', dest='host', required=True, help="Databricks Server URL")
        parser.add_argument('-t', '--token', dest='token', required=True, help="Databricks User Token")
        parser.add_argument('-j', '--jobType', dest='job_type', default='infoworks', help="Type of databricks jobs to be cleaned (all|infoworks)")


        parse_input = parser.parse_args()
        if not parse_input.host or not parse_input.token:
            print("Databricks credentials not provided")
            parser.print_help()
            sys.exit(1)

        return parse_input


if __name__ == '__main__':
    input = Input()
    parse_input = input.get()
    dbObj = Databricks(host=parse_input.host,token=parse_input.token)
    if parse_input.job_type == 'infoworks':
        try:
            infoworks_home = os.environ['IW_HOME']
            sys.path.insert(0, "{}/apricot-meteor/infoworks_python".format(infoworks_home))
        except KeyError:
            raise RuntimeError('IW_HOME not set. Please source IW_HOME/bin/env.sh and rerun script')
    dbObj.delete_jobs(parse_input.job_type)

