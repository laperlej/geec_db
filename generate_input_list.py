"""takes a database and outputs a tab delimited file with format: file_path\tmd5sum\n
"""

import sys
import sqlite3
import json

class IhecDb(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def get_json_view(self):
        sql = """SELECT datasets.file_path,
                        datasets.md5sum
                 FROM datasets"""
        self.cursor.execute(sql)
        return self.cursor.fetchall()

def process_datasets(datasets):
    processed_datasets = []
    for dataset in datasets:
        data = {
            'file_path': dataset[0],
            'md5sum': dataset[1],
        }
        processed_datasets.append(data)
    return processed_datasets
    


def main():
    sqlite_path = sys.argv[1]
    out_path = sys.argv[2]
    ihec_db = IhecDb(sqlite_path)
    datasets = ihec_db.get_json_view()
    datasets = process_datasets(datasets)
    with open(out_path, 'w') as output_file:
        for dataset in datasets:
            output_file.write('{0}\t{1}\n'.format(dataset['file_path'], dataset['md5sum']))

if __name__ == '__main__':
    main()