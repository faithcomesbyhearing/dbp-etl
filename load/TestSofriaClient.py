import sys
import subprocess
from Config import *

class TestSofriaClient:
    def __init__(self, config, testDirectory, generateJsonPath, populateDbPath):
        self.config = config
        self.testDirectory = testDirectory
        self.generateJsonPath = generateJsonPath
        self.populateDbPath = populateDbPath
        self.sofriaClient = config.sofria_client_js
        print("sofriaClient: ", self.sofriaClient)

    def execute(self, program):
        cmd = self.command(program)
        print("cmd:", cmd)
        response = subprocess.run(cmd, shell=False, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=2400)
        success = response.returncode == 0
        print(str(response.stdout.decode('utf-8')))

        if success == True:
            print("Success")
        else:
            print("stderr:", str(response.stderr.decode('utf-8')))
   
    def command(self, program):
        if program == "run":
            return [self.config.node_exe,
            self.sofriaClient,
            "run",
            self.testDirectory,
            "--populate-db=%s" % (self.populateDbPath),
            "--generate-json=%s" % (self.generateJsonPath)]
        else:
            print("ERROR: Unknown program %s" % (program))
            return None

if len(sys.argv) < 5:
    print("Usage: python3 load/TestSofriaClient.py [config_profile root_directory] [generate_json_path] [populate_db_path]")
    sys.exit()

rootDirectory = sys.argv[2]
generateJsonPath = sys.argv[3]
populateDbPath = sys.argv[4]

config = Config()
test = TestSofriaClient(config, rootDirectory, generateJsonPath, populateDbPath)

test.execute("run")

# python3 load/TestSofriaClient.py test ~/files/upload/TUKTTVP_ET-usx ~/files/accepted/TUKTTVP_ET-json ~/files/accepted/TUKTTVP_ET.db
