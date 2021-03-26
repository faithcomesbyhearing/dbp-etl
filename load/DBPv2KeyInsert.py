# DBPv2KeyInsert.py

# This onetime program is used to load keys from DBPv2 over to DBPv4.

import csv
from SQLBatchExec import *

class DBPv2KeyInsert:

	def __init__(self, dbOut):
		self.dbOut = dbOut
		print("init DBPv2KeyInsert")


	def process(self, filename):
		insertUsers = []
		insertKeys = []
		insertGroups = []
		with open(filename, encoding="utf-8", newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				key = row["key"]
				userEmail = row["user_email"]
				displayName = row["display_name"]
				password = "unknown"
				activated = 1  ## I assume this should be activate
				token = "unknown"  ## I don't know what this is. there are 26 unique value in the users table
				insertUsers.append((displayName, userEmail, password, activated, token))
				userId = "@" + userEmail  # userEmail is unique in the users table.
				insertKeys.append((userId, key, displayName))
				userKeyId = "@" + userId
				insertGroups.append((121, userKeyId))
				insertGroups.append((123, userKeyId))
				insertGroups.append((125, userKeyId))

		self.dbOut.insert("users", [], ["name", "email", "password", "activated", "token"], insertUsers, 1)
		self.dbOut.insert("user_keys", [], ["user_id", "key", "name"], insertKeys, 0)
		self.dbOut.insert("access_group_api_keys", [], ["access_group_id", "key_id"], insertGroups)

		## Is there really correspondance between users and user_keys over the id
		## The following query makes it seem that this correspndance does not exist
		## select u.id, u.v2_id, u.name, uk.name from users u, user_keys uk where u.id=uk.user_id

if (__name__ == '__main__'):
	if len(sys.argv) != 3:
		print("Usage: load/DBPv2KeyInsert.py  config_profile  filename.csv")
		sys.exit()
	csvFilename = sys.argv[2]
	config = Config()
	dbOut = SQLBatchExec(config)
	keys = DBPv2KeyInsert(dbOut)
	keys.process(csvFilename)
	dbOut.displayCounts()
	dbOut.displayStatements()
	#dbOut.execute("userkeys")





