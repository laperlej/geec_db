import os
import sys
import json
import sqlite3
import glob

DB_ROOT = os.path.dirname(__file__)
DB_PATH = DB_ROOT + 'ihec_db.sqlite'
CRE_SCRIPT = DB_ROOT + 'create.sql'
IHEC_ROOT = "/nfs3_ib/10.4.217.32/home/genomicdata/ihec_datasets/2016-03"

def read_json(json_path):
    json_content = {}
    try:
        with open(json_path, 'r') as json_file:
            json_text = json_file.read()
            json_content = json.loads(json_text)
    except IOError:
        print "Could not open file {0}".format(json_path)
        exit(1)
    return json_content


class IhecDb(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.create_db()

    def __del__(self):
        self.conn.close()

    def create_db(self):
        create_script = open(CRE_SCRIPT, 'r').read()
        self.cursor.executescript(create_script)
        self.commit()

    def insert_json(self, json_content, group):
        hub_description = json_content['hub_description']
        hub_id = self.insert_hub(hub_description)
        self.insert_datasets(json_content['datasets'], hub_description["assembly"], hub_id, group)

    def insert_hub(self, hub_description):
        assembly = hub_description["assembly"]
        publishing_group = hub_description["publishing_group"].lower()
        release_date = hub_description["date"]

        tuple_to_insert = (None, assembly, publishing_group, release_date)
        sql_query = 'INSERT OR IGNORE INTO hub_description VALUES (?, ?, ?, ?)'
        self.cursor.execute(sql_query, tuple_to_insert)
        self.commit()
        return self.cursor.lastrowid

    def insert_datasets(self, datasets, assembly, hub_id, group):
        for dataset in datasets:
            metadata = datasets[dataset]
            try:
                self._insert_dataset(metadata, assembly, hub_id, group)
            except KeyError as e:
                pass
        self.commit()

    def _insert_dataset(self, dataset, assembly, hub_id, group):
        file_name = dataset["ihec_data_portal"]["local_files"]["signal"]["file_name"]
        md5sum = dataset["ihec_data_portal"]["local_files"]["signal"]["md5sum"]
        assay = dataset["ihec_data_portal"]["assay"]
        assay_category = dataset["ihec_data_portal"]["assay_category"]
        cell_type = dataset["ihec_data_portal"]["cell_type"]
        cell_type_category = dataset["ihec_data_portal"]["cell_type_category"]
        #analysis_group = dataset["analysis_attributes"]["analysis_group"].lower()
        analysis_group = group

        file_path = "{0}/{1}/{2}/{3}".format(IHEC_ROOT, analysis_group, assembly, file_name)

        tuple_to_insert = (None,
                           hub_id,
                           file_name,
                           file_path,
                           md5sum,
                           assay,
                           assay_category,
                           cell_type,
                           cell_type_category,
                           analysis_group)
        sql_query = 'INSERT OR IGNORE INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        self.cursor.execute(sql_query, tuple_to_insert)
        return self.cursor.lastrowid

    def commit(self):
        self.conn.commit()

def extract_analysis_group(full_path):
    return os.path.basename(full_path).split('.')[0]

def extract_json(json_path):
    group = extract_analysis_group(json_path)
    json_content = read_json(json_path)
    ihec_db = IhecDb(DB_PATH)
    ihec_db.insert_json(json_content, group)

def main():
    json_path_regex = sys.argv[1]
    for json_path in glob.glob(json_path_regex):
        print json_path
        extract_json(json_path)

if __name__ == '__main__':
    main()
