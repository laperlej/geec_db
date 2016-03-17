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
        sql = """SELECT datasets.assay, 
                        datasets.assay_category,
                        datasets.cell_type,
                        datasets.cell_type_category,
                        datasets.analysis_group,
                        datasets.file_path,
                        datasets.file_name,
                        datasets.md5sum,
                        hub_description.publishing_group
                 FROM datasets
                 LEFT JOIN hub_description
                 ON datasets.hub_id = hub_description.hub_id"""
        self.cursor.execute(sql)
        print self.cursor.fetchall()

def list_2_json(datasets):
    json_content = {'datasets':[]}
    for dataset in datasets:
        data = {
            'assay': dataset[0],
            'assay_category': dataset[1],
            'cell_type': dataset[2],
            'cell_type_category': dataset[3],
            'analysis_group': dataset[4],
            'file_path': dataset[5],
            'file_name': dataset[6],
            'md5sum': dataset[7],
            'publishing_group': dataset[8],
        }
        json_content['datasets'].append(data)
    return json.dumps(json_content, separators=(',',':'), sort_keys=True)


def main():
    sqlite_path = sys.argv[1]
    out_path = sys.argv[1]
    ihec_db = IhecDb(sqlite_path)
    datasets = ihec_db.get_json_view
    json = list_2_json(datasets)
    with open(out_path, 'w') as output_file:
        output_file.write(json)

if __name__ == '__main__':
    main()