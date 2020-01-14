# This program collects verse meta data for all books and chapters that have this data
#

import io
import urllib2
import json

HOST2 = "https://dbt.io/"
#HOST4 = "https://api.dbp4.org"
HOST4 = "http://api.dev.v4.dbt.io"
KEY2 = "key=b37964021bdd346dc602421846bf5683&v=2"
KEY4 = "key=afcb0adb-5247-4327-832d-abeb316358f9&v=4&pretty=true&format=json"

def get(path, arguments):
	url = HOST4 + path + "?" + KEY4
	if arguments is not None and len(arguments) > 0:
		url += "&" + "&".join(arguments)
	try:
		response = urllib2.urlopen(url)
		data = response.read()
		return data
	except Exception, err:
		print "Error", path, arguments, err
		return None

#output = get("/timestamps", []) # good
#output = get("/timestamps/search", ["query='Jesus'"]) # Empty result
#output = get("/bible/equivalents", []) # good
#output = get("/bibles/filesets/ENGESVT2DA", []) # Error 422
#output = get("/bibles/filesets/ENGWEB/download", []) # Error 500
#output = get("/bibles/filesets/ENGWEB/copyright", []) # Error 422
#output = get("/bibles/filesets/media/types", []) # good
#output = get("/bibles/filesets/ENGWEB/podcast", []) # status 200, but not found
#output = get("/bibles", []) # good
#output = get("/bibles/ENGWEB", []) # good
#output = get("/bibles/ENGWEB/book", [])
#output = get("/bibles/books", [])
#output = get("/bibles/filesets/ENGWEBT2DA/books", [])
#output = get("/api/assets", [])
#output = get("/commentaries", [])
#output = get("/commentaries/{commentary_id}/chapters", [])
#output = get("/commentaries/{commentary_id}/sections", []) # 
#output = get("/lexicons", []) # 
#output = get("/bibles/ENGESV/book/chapter", []) # 
#output = get("/search", []) # 
#output = get("/alphabets", []) # 
#output = get("/alphabets/Arab", []) # 
#output = get("/languages", []) # 
#output = get("/languages/ENG", []) # 
#output = get("/numbers/range", []) # 
#output = get("/numbers", []) # 
#output = get("/numbers/bengali", []) # 
#output = get("/projects/34187/oauth-providers/", []) # 
#output = get("/projects", []) # 
#output = get("/projects/{project_id}", []) # 
#output = get("/accounts", []) # 
#output = get("/users", []) # 
#output = get("/users/{user_id}/bookmarks", []) # 
#output = get("/users/{user_id}/highlights", []) # 
#output = get("/users/{user_id}/notes", []) # 



print output



