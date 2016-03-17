import sys
import sqlite3

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
        print self.cursor.fetchone()


def main():
    sqlite_path = sys.argv[1]
    ihec_db = IhecDb(sqlite_path)

if __name__ == '__main__':
    main()