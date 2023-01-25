/* datasource.orange_book_patent */
DROP TABLE IF EXISTS datasource.orange_book_patent;

CREATE TABLE datasource.orange_book_patent (
Appl_Type                   TEXT,
Appl_No                     TEXT,
Product_No                  TEXT,
Patent_No                   TEXT,
Patent_Expire_Date_Text     TEXT,
Drug_Substance_Flag         TEXT,
Drug_Product_Flag           TEXT,
Patent_Use_Code             TEXT,
Delist_Flag                 TEXT,
Submission_Date             TEXT
);

COPY datasource.orange_book_patent
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_orange_book') }}/patent.txt' DELIMITER '~' CSV HEADER;