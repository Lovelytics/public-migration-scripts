import argparse
from utils.create_workspace import Workspace as Workspace

def main():
    # checkpoints are optional for export but you will need to use them for the import
    # (each workspace is a 'checkpoint')

    all_args = argparse.ArgumentParser()
    all_args.add_argument("--checkpoint", dest="checkpoint", default="", help="set if you are using a checkpoint during export")
    all_args.add_argument("--workspaces", dest="workspaces", nargs="+", required=True, help="list of workspace names. must match csv columns")

    args = all_args.parse_args()

    checkpoint = args.checkpoint
    workspaces = args.workspaces

    for w in workspaces:
        workspace = Workspace(checkpoint, w, workspaces)
        workspace.run()

    workspace.copy_other_files()

if __name__ == "__main__":
    main()
