import argparse
import os.path
import re
import sys
import subprocess
from multiprocessing import Pool


parser = argparse.ArgumentParser("Google Drive Uploader")
parser.add_argument("--src",
                    dest="src",
                    help="The source folder or file path in the local host",
                    type=str)
parser.add_argument("--dest",
                    dest="dest",
                    help="The destination folder like Folder 1 in the destination Google Drive",
                    type=str)
args = parser.parse_args()

mapper = {}


def execute_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    result = []
    for line in process.stderr:
        sys.stderr.write(line)
        sys.stderr.flush()
        result.append(line.strip())
    return result


def get_parent_id(id):
    results = execute_command("gdrive info " + id)
    for result in results:
        search = re.search('Parents: ([A-Za-z0-9-_]+)', result, re.IGNORECASE)
        if search:
            return search.group(1)

    return None


def get_dest(parent_id, dest):
    command = "gdrive list --query \"name = '" + dest + "'\" -m 1000"

    results = execute_command(command)
    ids = []

    for result in results:
        search = re.search('([A-Za-z0-9-_]+)[ ]+(\w+)[ ]+dir[ ]+[0-9- :]+', result, re.IGNORECASE)
        if search:
            ids.append(search.group(1))

    filtered_list = []
    if parent_id is not None:
        for id in ids:
            pid = get_parent_id(id)
            if pid is not None and pid == parent_id:
                filtered_list.append(id)
    else:
        filtered_list = ids

    if len(filtered_list) == 0:
        print("ID not found for %s" % dest)
        exit(-1)
    elif len(filtered_list) > 1:
        print("Multiple ID found for %s" % dest)
        exit(-1)

    return filtered_list[0]


def upload_file(file):
    parent_folder = os.path.abspath(os.path.join(src, os.pardir))
    if parent_folder not in mapper:
        print("Parent Id not found")
        exit(-1)

    parent_id = mapper[parent_folder]
    file_name = os.path.basename(file)
    id = get_dest(parent_id, file_name)
    if id is not None:
        print("File already uploaded: %s" % file_name)
        return

    results = execute_command("gdrive upload " + file + " --parent " + parent_id)
    for result in results:
        search = re.search('Parents: ([A-Za-z0-9-_]+)', result, re.IGNORECASE)
        if search:
            return search.group(1)

    return


if __name__ == "__main__":
    src = args.src.strip()
    dest = args.dest.strip()

    if src.endswith(os.sep):
        src = src[:-1]

    _ = execute_command("gdrive about")

    parent_id = get_dest(None, dest)
    if parent_id is None:
        print("Destination path not found: %s" % dest)

    if not os.path.exists(src):
        print("Source path not found: %s" % src)
        exit(-1)

    src = os.path.join(src)
    src_files = []
    mapper[os.path.abspath(os.path.join(src, os.pardir))] = parent_id

    if os.path.isdir(src):
        print("Source is a directory")

        src_folders = []

        for root, subdirs, files in os.walk(src):
            for subdir in subdirs:
                src_folders.append(os.path.join(root, subdir))
            for file in files:
                src_files.append(os.path.join(root, file))

        for src_folder in src_folders:
            folder_name = os.path.basename(src)
            parent_folder = os.path.abspath(os.path.join(src, os.pardir))
            id = get_dest(mapper[parent_folder], folder_name)
            if id is None:
                create_dir(parent_id, folder_name)

            print("Directory already created: %s" % folder_name)
            mapper[src] = id
    else:
        print("Source is a file")
        src_files.append(src)

    with Pool(50) as p:
        _ = p.map(upload_file, src_files)
