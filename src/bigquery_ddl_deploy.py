import datetime
import hashlib
import logging
import os
import subprocess
import sys

from pymongo import MongoClient
import mongo_utilities.mongo_manager as mongo_manager


class ProjectRepoHashes:
    def __init__(self, project_name, repo_name, google_project_id, hash_store_user, hash_store_password):
        # Set attributes
        self.hash_store_user = hash_store_user
        self.hash_store_password = hash_store_password
        self.project_name = project_name
        self.repo_name = repo_name
        self.google_project_id = google_project_id

        self.collection = self._setup_mongo_collection()

        self.hashes = self._get_hashes(
            self.project_name,
            self.repo_name,
            self.google_project_id
        )

        if not self.hashes:
            logging.warning(
                "No hash store exists for project %s repo %s google project %s.",
                self.project_name,
                self.repo_name,
                self.google_project_id,
            )
            logging.warning("Creating a new hash store.")
            self.hashes = {
                "project_name": self.project_name,
                "repo_name": self.repo_name,
                "google_project_id": self.google_project_id,
                "create_date": datetime.datetime.utcnow(),
                "files": [],
            }
        self.hashes["update_date"] = datetime.datetime.utcnow()
        self.filesdict = dict((f["file_name"], f) for f in self.hashes["files"])

    def _setup_mongo_collection(self):
        mongodb_url = mongo_manager.get_client_string(
            'hadoopDeploy',
            self.hash_store_user,
            self.hash_store_password,
            mongo_manager.Environments.PRODUCTION
        )
        client = MongoClient(mongodb_url)
        db = client.hadoopDeploy
        collection = db.bigQueryDeployHash
        return collection

    def _get_hashes(self, project_name, repo_name, google_project_id):
        return self.collection.find_one({
            "project_name": project_name,
            "repo_name": repo_name,
            "google_project_id": google_project_id,
        })

    def get_file_hash(self, file_name):
        if file_name in self.filesdict:
            return self.filesdict[file_name]["file_hash"]
        return ""

    def matches_saved_hash(self, file_name, text):
        logging.info("Checking for hash match on file %s", file_name)
        new_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
        old_hash = self.get_file_hash(file_name)

        if old_hash == "":
            logging.info("File %s not found in hash store", file_name)
        else:
            logging.debug("Old hash: %s", old_hash)
        logging.debug("New hash: %s", new_hash)

        if new_hash == old_hash:
            logging.info("Hashes match")
            return True
        else:
            logging.info("Hashes do not match")
            return False

    def update_saved_hash(self, file_name, text):
        file_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
        if file_name in self.filesdict:
            logging.info("update_saved_hash: updating hash for %s", file_name)
            file_attrs = self.filesdict[file_name]
            file_attrs["file_hash"] = file_hash
            file_attrs["deploy_date"] = datetime.datetime.utcnow()
        else:
            logging.info("update_saved_hash: adding new hash for %s", file_name)
            new_hash = {
                "file_name": file_name,
                "file_hash": file_hash,
                "deploy_date": datetime.datetime.utcnow(),
                "create_date": datetime.datetime.utcnow(),
            }
            self.hashes["files"].append(new_hash)
            self.filesdict[file_name] = new_hash

    def save(self):
        # if no id, then record is new
        logging.info("Saving hashes to database")
        if "_id" in self.hashes:
            self.collection.replace_one({"_id": self.hashes["_id"]}, self.hashes)
        else:
            self.collection.insert_one(self.hashes)

def deploy(google_project_id, ddl_folder_path, hash_store_user, hash_store_password):
    repo_owner_name = os.environ["REPO_AND_OWNER_NAME"].split('/', 1)

    hash_store = ProjectRepoHashes(
        project_name=repo_owner_name[0],
        repo_name=repo_owner_name[1],
        google_project_id=google_project_id,
        hash_store_user=hash_store_user,
        hash_store_password=hash_store_password,
    )

    ddl_files = find_ddl_files(ddl_folder_path)
    for ddl_file in ddl_files:
        sql = get_sql(ddl_file)
        if hash_store.matches_saved_hash(ddl_file, sql):
            logging.info("Skipping file %s. Matches saved file hash.", ddl_file)
            continue
        try:
            logging.info("Executing file %s.", ddl_file)
            execute_sql(sql, google_project_id)
        except subprocess.CalledProcessError as err:
            logging.error(
                "Error executing ddl file %s: return code %d", ddl_file, err.returncode
            )
            logging.error("stdout: %s", err.stdout)
            logging.error("stderr: %s", err.stderr)
            logging.error("Exiting due to SQL deployment error")
            sys.exit(3)

        # Only save hash if execution succeeded
        logging.info("Saving file hash for %s.", ddl_file)
        hash_store.update_saved_hash(ddl_file, sql)
        hash_store.save()

def get_sql(sql_file):
    with open(sql_file) as sql_fh:
        sql = sql_fh.read()
        return sql

def execute_sql(sql, google_project_id):
    bq_cmd = [
        "bq", "query",
        "--use_legacy_sql=false",
        "--project_id={0}".format(google_project_id),
    ]
    logging.info("Executing: %s", " ".join(bq_cmd))
    logging.info("Executing sql: %s", sql)
    result = subprocess.run(
        bq_cmd,
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        text=True,
    )
    logging.info("Execution complete")
    logging.info("stdout: %s", result.stdout)
    logging.info("stderr: %s", result.stderr)


def find_ddl_files(path):
    """
    Find DDL files in the given path.
    Returns a list
    """
    logging.debug("Finding DDL files in %s", path)
    try:
        files = os.listdir(path)
    except OSError as err:
        logging.error("Error listing DDL directory %s: %s", path, err.strerror)
        return []
    ddl_files = [
        os.path.join(path, entry)
        for entry in files
        if entry.endswith(".sql") and os.path.isfile(os.path.join(path, entry))
    ]
    ddl_files.sort()
    logging.debug("Found DDL files: %s", ", ".join(ddl_files))
    return ddl_files


def main():
    # Setup Variables
    google_project_id = os.environ["INPUT_GOOGLE_PROJECT_ID"]
    ddl_folder_path = os.environ["INPUT_DDL_FOLDER_PATH"]
    mongo_user = os.environ["INPUT_HADOOP_MONGO_USER"]
    mongo_password = os.environ["INPUT_HADOOP_MONGO_PASSWORD"]

    deploy(google_project_id, ddl_folder_path, mongo_user, mongo_password)

if __name__ == "__main__":
    main()