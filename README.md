# dbp-etl 

This program is used to load the DBP database for Faith Comes by Hearing from various sources.

To run this program one must have a dbp-etl.cfg file with the correct parameters in the HOME directory.  

Documentation on this is found in load/README.md

The program can be run in the following ways:

	python3 load/DBPLoadController.py  config_profile  data_location  filesetId1  filesetId2  FilesetIdi

	config_profile is a name like dev, test, stage, prod that is defined in your dbp-etl.cfg file.

	data_location can be a local path, such as D:/dir1/dir2 or /dir1/dir2 or it can be a bucket name, 
	such as s3://bucket_name

	filesetId can be any number of folders that are named after a filesetId, which contain files to be loaded.

To debug we can use the pdb library.

```python
import pdb; pdb.set_trace()
```
When you run your script, execution will pause at the pdb.set_trace() call. You'll be able to inspect variables, step through your code, and do other things. Here are some commands you might find useful:

- n(ext): Execute the next line.
- s(tep): Step into a function call.
- r(eturn): Continue execution until the current function returns.
- c(ontinue): Continue execution until a breakpoint is encountered.
- p(rint) <expression>: Evaluate the expression and print its value.
- q(uit): Quit the debugger and exit.
