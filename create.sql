CREATE TABLE IF NOT EXISTS hub_description (
    hub_id INTEGER PRIMARY KEY AUTOINCREMENT,
    assembly TEXT,
    publishing_group TEXT,
    release_date TEXT,
    UNIQUE (assembly, publishing_group, release_date)
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    hub_id INTEGER,
    file_name TEXT UNIQUE,
    file_path TEXT,
    md5sum TEXT,
    assay TEXT,
    assay_category TEXT,
    cell_type TEXT,
    cell_type_category TEXT,
    analysis_group TEXT
);
