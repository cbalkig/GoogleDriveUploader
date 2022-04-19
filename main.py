import argparse
import os.path
import re
import sys
import subprocess
from tqdm import tqdm


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

GDRIVE_PATH = "gdrive"
#GDRIVE_PATH = "C:\MyTools\GDrive\gdrive.exe"
LOG_FILE_PATH = "log.txt"
logs = []


def read_logs():
    processed_files = []
    if os.path.exists(LOG_FILE_PATH):
        f = open(LOG_FILE_PATH, "r")
        result = f.read()
        processed_files = result.split("\n")
        f.close()
    return processed_files


def execute_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    result = []
    if process.stderr is not None:
        for line in process.stderr:
            sys.stderr.write(line)
            sys.stderr.flush()
            result.append(line.strip())
    for line in process.stdout:
        result.append(line.strip())
    return result


def get_path(id):
    results = execute_command(GDRIVE_PATH + " info " + id)
    path = None
    for result in results:
        search = re.search('Path: (.+)', result, re.IGNORECASE)
        if search:
            path = search.group(1)
            break

    return path


def get_parent_id(id):
    results = execute_command(GDRIVE_PATH + " info " + id)
    pid = None
    path = None
    for result in results:
        search = re.search('Parents: ([A-Za-z0-9-_]+)', result, re.IGNORECASE)
        if search:
            pid = search.group(1)
            continue

        search = re.search('Path: (.+)', result, re.IGNORECASE)
        if search:
            path = search.group(1)
            continue

    return pid, path


def get_dest(parent_id, dest):
    if parent_id is None:
        command = GDRIVE_PATH + " list --query \"trashed = false and name = '" + dest + "'\" -m 1000"
    else:
        command = GDRIVE_PATH + " list --query \"trashed = false and name = '" + dest + "' and '" + str(parent_id) \
                  + "' in parents\" -m 1000"

    results = execute_command(command)
    ids = []

    for result in results:
        search = re.search('([A-Za-z0-9-_]+)[ ]+(.+)[ ]+(\s+)[ ]+[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}',
                           result, re.IGNORECASE)
        if search:
            ids.append((search.group(1), get_path(search.group(1))))

    if len(ids) == 0:
        return (None, None)
    elif len(ids) > 1:
        print("Multiple ID found for %s" % dest)

    return ids[0]


def upload_file(file):
    global logs
    if str(file) in logs:
        return

    parent_folder = os.path.abspath(os.path.join(file, os.pardir))
    if parent_folder not in mapper:
        print("Parent Id not found")
        exit(-1)

    parent_id = mapper[parent_folder]
    file_name = os.path.basename(file)
    id, path = get_dest(parent_id, file_name)
    if id is not None:
        #print(file + ": Already uploaded to " + path)
        append_to_log(file)
        return

    results = execute_command(GDRIVE_PATH + " upload '" + file + "' --parent " + parent_id)
    for result in results:
        search = re.search('Uploaded ([A-Za-z0-9-_]+) .*', result, re.IGNORECASE)
        if search:
            #print(file + ": Uploaded")
            append_to_log(file)
            return

    append_to_log(file)
    print(file + ": " + results)


def create_dir(parent_id, folder_name):
    results = execute_command(GDRIVE_PATH + " mkdir " + folder_name + " --parent " + parent_id)
    for result in results:
        search = re.search('Directory ([A-Za-z0-9-_]+) created', result, re.IGNORECASE)
        if search:
            return True

    return False


def append_to_log(file):
    global logs
    logs.append(file)
    if len(logs) % 100 == 0:
        f = open(LOG_FILE_PATH, "w")
        for log in logs:
            f.write(log + "\n")
        f.close()


if __name__ == "__main__":
    src = args.src.strip()
    dest = args.dest.strip()

    if src.endswith(os.sep):
        src = src[:-1]

    about_result = execute_command(GDRIVE_PATH + " about")
    print(about_result)

    logs = read_logs()

    parent_id, path = get_dest(None, dest)
    if parent_id is None:
        print("Destination path not found: %s" % dest)
        status = create_dir(None, dest)
        if status:
            print("Directory created: %s" % dest)

    if not os.path.exists(src):
        print("Source path not found: %s" % src)
        exit(-1)

    src = os.path.join(src)
    src_files = []
    mapper[os.path.abspath(os.path.join(src, os.pardir))] = parent_id

    if os.path.isdir(src):
        print("Source is a directory: %s" % src)

        src_folders = [src]

        for root, sub_dirs, files in os.walk(src):
            for subdir in sub_dirs:
                src_folders.append(os.path.join(root, subdir))
            for file in files:
                file_path = os.path.join(root, file)
                src_files.append(file_path)

        for src_folder in src_folders:
            folder_name = os.path.basename(src_folder)
            parent_folder = os.path.abspath(os.path.join(src_folder, os.pardir))
            id, path = get_dest(mapper[parent_folder], folder_name)
            if id is None:
                status = create_dir(mapper[parent_folder], folder_name)
                if status:
                    print("Directory created: %s" % folder_name)
                    i, _ = get_dest(mapper[parent_folder], folder_name)
                    id = i
            else:
                print("Directory already created:", folder_name, ":", path)
            mapper[src_folder] = id
    else:
        print("Source is a file: %s" % src)
        src_files.append(src)

    for i in tqdm(range(len(src_files))):
        src_file = src_files[i]
        upload_file(src_file)
