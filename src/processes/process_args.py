from dataclasses import dataclass
import sys
from typing import Optional

from dataclasses_json import dataclass_json

from server_core.db.db_connection import DbConnection


@dataclass_json
@dataclass
class ProcessArgs:
    user_id: Optional[int] = None
    guid: Optional[str] = None
    service_script: Optional[str] = None
    service_script_id: Optional[int] = None
    env: Optional[str] = None
    custom_data: Optional[str] = None
    keep_record: bool = False
    allow_multiple: bool = False
    debug: bool = False
    update_mem: bool = False
    command_line: Optional[str] = None
    
    @staticmethod
    def from_command_line_args():
        
        process_args = ProcessArgs(command_line=str(sys.argv))
        
        for i in range(len(sys.argv)):
            if sys.argv[i] == "-u" or sys.argv[i] == "--user_id":
                process_args.user_id = int(sys.argv[i + 1])
            if sys.argv[i] == "-g" or sys.argv[i] == "--guid":
                process_args.guid = sys.argv[i + 1]
            if sys.argv[i] == "-s" or sys.argv[i] == "--script":
                process_args.service_script = sys.argv[i + 1]
            if sys.argv[i] == "-i" or sys.argv[i] == "--script_id":
                process_args.service_script_id = int(sys.argv[i + 1])
            if sys.argv[i] == "-e" or sys.argv[i] == "--env":
                process_args.env = sys.argv[i + 1]
            if sys.argv[i] == "-c" or sys.argv[i] == "--custom_data":
                process_args.custom_data = sys.argv[i + 1]
            if sys.argv[i] == "-k" or sys.argv[i] == "--keep":
                process_args.keep_record = True
            if sys.argv[i] == "-x" or sys.argv[i] == "--allow_multiple":
                process_args.allow_multiple = True
            if sys.argv[i] == "-d" or sys.argv[i] == "--debug":
                process_args.debug = True
            if sys.argv[i] == "-m" or sys.argv[i] == "--mem":
                process_args.update_mem = True
                
        return process_args
    
    def get_name(self):
        if self.service_script is not None:
            return self.service_script
        if self.service_script_id is not None:
            return str(self.service_script_id)
        raise Exception('Unknown name')
    
    def get_script(self, conn: DbConnection):
        if self.service_script is not None:
            return self.service_script
        if self.service_script_id is not None:
            return conn.select_one("""
                                SELECT command
                                    FROM gb_app.def_server_services
                                WHERE id = %s
                                """, [self.service_script_id])['command']
        raise Exception('Unknown script')
    
    def to_command_line_args(self):
        params = []
        
        if self.user_id is not None:
            params.append('-u')
            params.append(str(self.user_id))
            
        if self.guid is not None:
            params.append('-g')
            params.append(self.guid)
            
        if self.service_script is not None:
            params.append('-s')
            params.append(self.service_script)

        if self.service_script_id is not None:
            params.append('-i')
            params.append(str(self.service_script_id))

        if self.env is not None:
            params.append('-e')
            params.append(self.env)

        if self.custom_data is not None:
            params.append('-c')
            params.append(self.custom_data)

        if self.keep_record:
            params.append('-k')

        if self.allow_multiple:
            params.append('-x')

        if self.debug:
            params.append('-d')

        if self.update_mem:
            params.append('-m')
    
        return params
