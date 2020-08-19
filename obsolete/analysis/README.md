Instructions for running dbp-etl developmental programs.

Prepare developmental databases

1) Create a database dbp, that will contain production data.
2) Create a database dbp_only, that will contain production data, but exclude dbs.
3) Create a database named valid_dbp, that will be used to store the developmental data
4) Populate each database with the full production mysqldump.
5) run the script sh table_delete.sh, it will delete data from all tables to be generated in valid_dbp
6) modify a temp copy of table_delete.sh to delete dbp_only, and run it.
7) run the sql scrip remove_digital_bible_society.sql

Each of the programs with the name “….Table.py” is currently a standalone program that populates the table with the same name using data from the bucket listings and LPTS.

