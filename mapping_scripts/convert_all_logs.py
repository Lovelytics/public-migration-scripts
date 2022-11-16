###################### importing other scripts ##############################################
from utils import to_csv as util
from utils import create_asset_mapping_spreadsheet as create_spreadsheet
############################################################################################
import argparse
import os

def main(checkpoint):
    # where you want the csv files to be located
    # make the csv directory if it not there
    if "csv" not in os.listdir():
        os.mkdir("./csv")

    # users
    users_data = util.read_log("users.log", checkpoint)
    users_df = util.create_users(users_data)
    util.save_to_csv(users_df, "users.csv")

    # instance profiles
    ip_data = util.read_log("instance_profiles.log", checkpoint)
    ip_df = util.create_instance_profiles(ip_data)
    util.save_to_csv(ip_df, "instance_profiles.csv")

    ipo_data = util.read_log("instance_pools.log", checkpoint)
    ipo_df = util.create_instance_pools(ipo_data)
    util.save_to_csv(ipo_df, "instance_pools.csv")

    # groups
    groups_df = util.create_groups(checkpoint, directory_name = "groups")
    util.save_to_csv(groups_df, "groups.csv")

    # clusters
    clusters_data = util.read_log("clusters.log", checkpoint)
    clusters_df = util.create_clusters(clusters_data)
    util.save_to_csv(clusters_df, "clusters.csv")

    # job
    jobs_data = util.read_log('jobs.log', checkpoint)
    jobs_df = util.create_jobs(jobs_data)
    util.save_to_csv(jobs_df, "jobs.csv")

    # shared
    shared_df = util.create_shared_logs(checkpoint, directory_name = "artifacts/shared")
    util.save_to_csv(shared_df, 'shared_logs.csv')

    # other artificats
    other_df = util.create_other_artifacts(checkpoint, directory_name = "artifacts")
    util.save_to_csv(other_df, "top_level_artifacts.csv")

    # libraries
    libraries_data = util.read_log("libraries.log", checkpoint)
    libraries_df = util.create_libraries(libraries_data)
    util.save_to_csv(libraries_df, "libraries.csv")

    # secret scopes
    scopes_df = util.create_scopes(checkpoint, directory_name = 'secret_scopes')
    util.save_to_csv(scopes_df, "secret_scopes.csv")

    # metastore
    metastore_df = util.create_metastore(checkpoint, directory_name = 'metastore')
    util.save_to_csv(metastore_df, "metastore.csv")

    create_spreadsheet.csv_to_excel("./csv")

if __name__ == "__main__":


    all_args = argparse.ArgumentParser()
    all_args.add_argument("--checkpoint", dest="checkpoint", default="", help="set if you are using a checkpoint during export")

    args = all_args.parse_args()
    main(args.checkpoint)
