import os
import subprocess

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.env_data import ENV_DATA


class ServicesDbModel(BaseDbModel):    
    
    def __init__(self, conn: DbConnection = DbConnection()):
        super(ServicesDbModel, self).__init__(conn=conn)
        
    def run(
        self,
        u: int | None = None,
        g: str | None = None,
        d: int | None = None,
        s: str | None = None,
        e: str | None = None,
        m: str | None = None,
        c: str | None = None,
        a=False,
        x=False,
        z=False
    ):
        """
        :param u: user id
        :param g: guid (auto generated if None)
        :param d: service def id
        :param s: service script path
        :param e: environment (dev, prod)
        :param m: method to run (default is default_method())
        :param c: custom data
        :param a: do not delete service record from db after successful run
        :param x: run service even if service with same id / script is already running
        :param b: set debug env
        :param z: update shared memory if already running
        :return: nothing
        """
        params = []

        if u is not None:
            params.append("-u")
            params.append(str(u))
        if g is not None:
            params.append("-g")
            params.append(g)
        if d is not None:
            params.append("-d")
            params.append(str(d))
        if s is not None:
            params.append("-s")
            params.append(s)
        if e is not None:
            params.append("-e")
            params.append(e)
        if m is not None:
            params.append("-m")
            params.append(m)
        if c is not None:
            params.append("-c")
            params.append(c)
        if a:
            params.append("-a")
        if x:
            params.append("-x")
        if z:
            params.append("-z")
        if ENV_DATA.is_debug():
            params.append("-b")

        root = Utils.get_project_root_path()

        if os.name == 'nt':
            stdout = None
            stderr = None
            preexec_fn = None
            data = [os.path.join(root, "conda", "python.exe"), os.path.join(root, "service_wrapper.py")] + params
        else:
            stdout = open('/dev/null', 'w')
            stderr = open('logfile.log', 'a')
            preexec_fn = os.setpgrp
            data = ["nohup", "bash", os.path.join(root, "service_wrapper.sh")] + params

        print(f"[debug] service: subprocess open {' '.join(data)}")
        subprocess.Popen(
            data,
            stdout=stdout,
            stderr=stderr,
            preexec_fn=preexec_fn)
    
