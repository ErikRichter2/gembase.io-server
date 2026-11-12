import os
import shutil
from time import sleep
import traceback
from typing import Optional

from server_core.db.db_connection import DbConnection
from server_core.logs.logs_db_model import LogsDbModel
from server_core.utils.utils import Utils
from src.db.gpt_db_model import GptDbModel
from src.db.trader_db_model import TraderDbModel
from src.db.trading_db_model import TradingDbModel
from src.db.workflow_db_model import WorkflowDbModel
from src.env_data import ENV_DATA
from src.processes.gpt_news_worker import GptNewsWorker
from src.processes.process_args import ProcessArgs
from src.processes.process_utils import ProcessUtils

class Cron:
    
    def __init__(self):
        self.conn = DbConnection()
        
    def log_error(self, e, name: Optional[str] = None):
        LogsDbModel().server_error_log(
            user_id=0,
            title=f'CRON - {name} - {str(e)}',
            stacktrace=traceback.format_exc()
        )
        
    def try_run(self, f, name: Optional[str] = None):
        try:
            f()
        except Exception as e:
            self.log_error(e=e, name=name)
        
    def daily(self):
        
        # ############# #
        # run daily job #
        # ############# #

        WorkflowDbModel(conn=self.conn).start_new_job(job_template_id=1)
               
        # ###################################### #
        # remove old files in /srv/upload folder #
        # ###################################### #
        
        try:
            p = '/srv/upload'
            for f in [os.path.join(p, f) for f in os.listdir('/srv/upload') if os.path.isfile(os.path.join(p, f))]:
                if os.path.getmtime(f) <= Utils.timestamp() - (3 * 24 * 60 * 60):
                    os.remove(f)
        except Exception as e:
            self.log_error(e, 'remove_old_upload_files')
                
        # ########################### #
        # remove expired files in dms #
        # ########################### #
        
        try:
            rows_dms = self.conn.select_all("""
                                    SELECT client_id
                                        FROM algo_dms.dms
                                        WHERE updated_t < DATE_SUB(NOW(), INTERVAL expire_days DAY)
                                        AND expire_days > 0
                                    """)
            
            for row in rows_dms:
                p = os.path.join(ENV_DATA.algo_data_base_dir, row['client_id'])
                shutil.rmtree(p, ignore_errors=True)
                self.conn.query("""
                                DELETE FROM algo_dms.dms
                                WHERE client_id = %s
                                """, [row['client_id']])
                self.conn.commit()
        except Exception as e:
            self.log_error(e, 'remove_old_dms_files')
            
        # ########################## #
        # delete old binance ws data #
        # ########################## #
        
        self.conn.query("""
                        DELETE FROM algo_data.binance_klines_ws
                         WHERE open_t < DATE_SUB(NOW(), INTERVAL 3 DAY)
                        """)
        self.conn.commit()
            
        self.conn.query("""
                        DELETE FROM algo_data.binance_order_book_ws
                         WHERE t < DATE_SUB(NOW(), INTERVAL 3 DAY)
                        """)
        self.conn.commit()
            
    def hourly(self):

        # ############# #
        # run hourly job #
        # ############# #

        WorkflowDbModel(conn=self.conn).start_new_job(job_template_id=2)

        # #################### #
        # News scraper - scrap #
        # #################### #
        
        cnt = ProcessUtils.process_running_instances(conn=self.conn, process_name='news_scraper')
        if cnt == 0:
            ProcessUtils.run(
                script='src/processes/news_scraper_worker.py',
                process_name='news_scraper',
                max_instances=1,
            )
            
        # ############################## #
        # check for finished gpt batches #
        # ############################## #
        
        self.try_run(lambda: GptDbModel.check_for_finished_batches(), 'check_for_finished_batches')
        self.try_run(lambda: GptNewsWorker.process_finished_batch(), 'GptNewsWorker.process_finished_batch')
        
        # ################################ #
        # refresh positions from exchanges #
        # ################################ #

        try:
            rows_traders = self.conn.select_all("""
                SELECT trader_id
                FROM algo_traders.traders
                WHERE state = 'working'
            """)
            for row in rows_traders:
                TraderDbModel(conn=self.conn, trader_id=row['trader_id']).refresh_exchange_positions()
        except Exception as e:
            self.log_error(e, 'trader_refresh_positions')
        
    def hourly_4(self):

        # ############# #
        # run every 4 hours job excluding 00:00 #
        # ############# #
        
        workflow_db_model = WorkflowDbModel(conn=self.conn)

        # run only when job=2 (hourly) finsihed for current hour
        try_cnt = 0        
        while try_cnt <= 5 * 60:
            row_job = self.conn.select_one_or_none("""
                                        SELECT MAX(j.id) as id
                                          FROM algo_workflow.jobs j
                                         WHERE j.job_template_id = 2
                                           AND DATE_FORMAT(j.t, '%Y-%m-%d %H:00:00') = DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00')
                                        """)
            if row_job is not None and workflow_db_model.is_job_finished(job_instance_id=row_job['id']):
                break
            
            sleep(1)
            try_cnt += 1
            
        workflow_db_model.start_new_job(job_template_id=3)                    

    def daily_utc(self):
        WorkflowDbModel(conn=self.conn).start_new_job(job_template_id=5)
        
    def job_15min(self):
        WorkflowDbModel(conn=self.conn).start_new_job(job_template_id=12)

    def job_8am(self):
        WorkflowDbModel(conn=self.conn).start_new_job(job_template_id=13)

def process_main(args: ProcessArgs):
    c = Cron()
    
    try:
        
        if args.custom_data == "daily":
            c.daily()        
        if args.custom_data == "daily_utc":
            c.daily_utc()        
        if args.custom_data == "hourly":
            c.hourly()
        if args.custom_data == "4_hourly":
            c.hourly_4()
        if args.custom_data == "15min":
            c.job_15min()
        if args.custom_data == "8am":
            c.job_8am()
            
    except Exception as e:
        LogsDbModel().server_error_log(
            user_id=0,
            title=f'CRON - {str(e)}',
            stacktrace=traceback.format_exc()
        )
        
    c.conn.close()
