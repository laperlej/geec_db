"""takes a database and outputs a json file for geec_web
"""

import sys
import sqlite3

class IhecDb(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def get_csv_view(self):
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
        return self.cursor.fetchall()

def list_2_csv(datasets):
    header = "datasets, #, repBiol, cellTypeCategory, cellType, assayCategory, assay, consortium"
    csv_content = []
    count = 1
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
        csv_line = [data['md5sum'], str(count), '{0}_{1}'.format(data['assay'],data['cell_type']).replace(' ', '_'), data['cell_type_category'], data['cell_type'], data['assay_category'], data['assay'], data['analysis_group']]
        csv_content.append(','.join(csv_line))
        count += 1
    return header + '\n'.join(csv_content)


def main():
    sqlite_path = sys.argv[1]
    out_path = sys.argv[2]
    ihec_db = IhecDb(sqlite_path)
    datasets = ihec_db.get_csv_view()
    csv = list_2_csv(datasets)
    with open(out_path, 'w') as output_file:
        output_file.write(csv)

if __name__ == '__main__':
    main()
