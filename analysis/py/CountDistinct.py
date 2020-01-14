#
# This program lists that tables and columns of a database along with their table row counts,
# and count of distinct values in each column.
#
import pymysql
import io

HOST = "127.0.0.1"
#PORT = 3320
PORT = 3306
#USER = "api_node_dbp"
USER = "root"
#PASS = "MeyynKbpTmduY7XUvdtMH88oU9fWa8"
PASS = "brilligg"
#DB = "dev_190614"
DB = "dbp"
DB = "test_dbp"


output = io.open("CountDistinct.out", mode="w", encoding="utf-8")

db = pymysql.connect(host = HOST,
					port = PORT,
					user = USER,
                    password = PASS,
                    db = DB,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor)
cur = db.cursor()

tblSelect = "select table_name from information_schema.tables where table_schema='%s' order by table_name;" % DB
cur.execute(tblSelect)
tblResult = cur.fetchall()
for tbl in tblResult:
	table = tbl['TABLE_NAME']
	#print table
	cur.execute("select count(*) as count from " + table)
	cntResult = cur.fetchone()
	print table, cntResult['count']
	output.write("%s, %d\n" % (table, cntResult['count']))

	colSelect = "select column_name, data_type, is_nullable from information_schema.columns where table_name='" + table + "' order by ordinal_position;"
	cur.execute(colSelect)
	colResult=cur.fetchall()

	for row in colResult:
		column = row['COLUMN_NAME']
		#print "\t", column
		countSelect = "select count(distinct `" + column + "`) as count from " + table
		cur.execute(countSelect)
		result = cur.fetchone()
		print "\t", column, result['count']
		output.write("\t%s, %d\n" % (column, result['count']))


cur.close()
db.close()
output.close()	
		
