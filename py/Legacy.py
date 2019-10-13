# Legacy

#
# This class contains any functions that are not part of the production build once this goes into production
#

import hashlib
from SQLUtility import *

class Legacy:


	def __init__(self, config):
		self.config = config
		self.filesetMap = None


	def hashId(self, bucket, filesetId, typeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def legacyFilesetMap(self):
		if self.filesetMap == None:
			prodDB = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_input_db_name)
			self.filesetMap = prodDB.selectMapList("SELECT concat(id, set_type_code), asset_id FROM dbp.bible_filesets", None)
			prodDB.close()
		return self.filesetMap

	def legacyAssetId(self, filesetId, setTypeCode, assetId):
		key = filesetId + setTypeCode
		legacyIds = self.legacyFilesetMap().get(key)
		if legacyIds == None:
			#print("no legacy bucket found actual one returned")
			return assetId
		elif len(legacyIds) == 1:
			return legacyIds[0]
		else:
			#print("multiple Ids %s  %s  %s" % (filesetId, setTypeCode, ",".join(legacyIds)))
			# When there are two the first is always dbp-prod, the second is dbp-dbs
			return legacyIds[0]


