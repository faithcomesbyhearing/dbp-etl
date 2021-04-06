# DBPv2KeyInsert.py

# This onetime program is used to load keys from DBPv2 over to DBPv4.

import re
import csv
from SQLUtility import *
from SQLBatchExec import *

class DBPv2KeyInsert:

	def __init__(self, config, dbOut):
		self.dbOut = dbOut
		db = SQLUtility(config)
		resultSet = db.select("SELECT email from dbp_users.users WHERE email is not NULL", ())
		self.emailSet = set()
		for item in resultSet:
			self.emailSet.add(item[0].lower())
		self.keySet = db.selectSet("SELECT `key` from dbp_users.user_keys WHERE `key` is not NULL", ())
		db.close()


	def process(self, filename):
		insertUsers = []
		updateUsers = []
		insertKeys = []
		insertGroups = []
		with open(filename, encoding="utf-8", newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				key = row["user_key"]
				userEmail = row["user_email"]
				displayName = row["display_name"]
				firstName = row["first_name"]
				lastName = row["last_name"]
				password = "api developer"
				activated = 1  ## I assume this should be activate
				token = "api developer"
				notes = "inserted by DBPv2KeyInsert.py"
				userId = "@" + re.sub(r'[@\.\-]', '', userEmail) # userEmail is unique in the users table.
				keyId = "@" + key
				if userEmail.lower() in self.emailSet:
					print("Did not update user ", userEmail)
					dbOut.rawStatement("SELECT id INTO %s FROM dbp_users.users WHERE `email` = '%s';" % (userId, userEmail))
				else:
					sql = ("INSERT INTO dbp_users.users (email, v2_id, name, first_name, last_name, password, activated, token, notes)"
							" VALUES ('%s', 0, '%s', '%s', '%s', '%s', %s, '%s', '%s');" % (userEmail, displayName, firstName, lastName, password, activated, token, notes))
					dbOut.rawStatement(sql)
					dbOut.rawStatement("SET %s = LAST_INSERT_ID();" % (userId))
				if key in self.keySet:
					print("ERROR Duplicate key ", key, "Cannot add key for", userEmail)
				else:
					sql = "INSERT INTO dbp_users.user_keys (`user_id`, `key`, `name`) VALUES (%s, '%s', '%s');" % (userId, key, displayName)
					dbOut.rawStatement(sql)
					dbOut.rawStatement("SET %s = LAST_INSERT_ID();" % (keyId))
				for priv in [121, 123, 125]:
					sql = "INSERT INTO dbp_users.access_group_api_keys (`access_group_id`, `key_id`) VALUES (%s, %s);" % (priv, keyId)
					dbOut.rawStatement(sql)


if (__name__ == '__main__'):
	if len(sys.argv) != 3:
		print("Usage: load/DBPv2KeyInsert.py  config_profile  filename.csv")
		sys.exit()
	csvFilename = sys.argv[2]
	config = Config()
	dbOut = SQLBatchExec(config)
	keys = DBPv2KeyInsert(config, dbOut)
	keys.process(csvFilename)
	dbOut.displayCounts()
	dbOut.displayStatements()
	dbOut.execute("userkeys")

# python3 load/DBPv2KeyInsert.py newdata $HOME/Desktop/query_result.csv

# ALTER TABLE dbp_users.access_group_api_keys DROP FOREIGN KEY access_group_api_keys_ibfk_1;
# ALTER TABLE dbp_users.access_group_api_keys ADD CONSTRAINT `access_group_api_keys_ibfk_1` FOREIGN KEY (`access_group_id`) REFERENCES `dbp`.`access_groups` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;


# delete from access_group_api_keys where key_id in (select id from user_keys where user_id in (select id from users where notes = 'inserted by DBPv2KeyInsert.py'))
# delete from user_keys where user_id in (select id from users where notes = 'inserted by DBPv2KeyInsert.py')
# delete from users where notes = 'inserted by DBPv2KeyInsert.py'
"""
## Creating CSV file to load into this program

#create temporary table the_key_rows
#select * from wp_usermeta where meta_key = 'dbt_acct_key' and meta_value in
#('d848a9ba40ae43a05b768670c41c8e3f',
#'6ce262a6e6312e0175d20279f1ad3010', 
#'809db3bd83c66f3bc41be0cbd6bd1e3f', 
#'b56b3f1db611383681cf4e8018bacbc3', 
#'3e0eed1a69fc6e012fef51b8a28cc6ff', 
#'53355c32fca5f3cac4d7a670d2df2e09', 
#'e4a78850aaa914bea244b6482d6a0cc2', 
#'fc34da4152d8e26ac14450eb16728e33', 
#'2318647b4eb05214c60b616b54b13e6d', 
#'8d7dd2ba4835d5e0be95b2c88c0b41f7', 
#'c0ffa1c4578fe8463ef380b009754462', 
#'cbf509eda05af795148b17d959710e94', 
#'00ace9b063e81fd15035683e33d34a2f', 
#'79b63bfcda6671ecd7a6659087952b78', 
#'c9c1b6a538a418d983827560d8858c8c', 
#'c6493269b12694dd833b6bb2b7a3bb48', 
#'d3c2b792c987627250d1eb3655aec4b3');

create temporary table the_key_rows
select * from wp_usermeta where meta_key = 'dbt_acct_key' and meta_value in
('1cceebc9e9babcfdca12ddd2388ec35a',
'e094324a8e5d592f8b68c0e767cd88ce',
'ec9e8df857b349e8c5130c353af62f5d',
'aa129d3e47ddf57e8ffcc5b90a5abac6',
'88f2ec888286012bd016dfbb6b191bce',
'4e36f9bd5338ec10d7519bdf328551b7',
'68f0e1f583e0546c352b571c0478eddd');

select * from the_key_rows;

select k.user_id, k.meta_value, u.user_email, u.display_name
from the_key_rows k
join wp_users u on u.id = k.user_id

select k.user_id, k.meta_value as user_key, u.user_email, u.display_name, fn.meta_value as first_name, ln.meta_value as last_name
from the_key_rows k
join wp_users u on u.id = k.user_id
left outer join wp_usermeta fn on u.id = fn.user_id
left outer join wp_usermeta ln on u.id = ln.user_id
where fn.meta_key = 'first_name'
and ln.meta_key = 'last_name'

"""



