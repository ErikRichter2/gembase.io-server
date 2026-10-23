import json

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.user_db_model import UserDbModel
from src.env_data import ENV_DATA
from src.models.emails.email_sender import EmailSender


class AdminDbModel(UserDbModel):

    def __init__(self, conn: DbConnection, user_id: int):
        super(AdminDbModel, self).__init__(conn=conn, user_id=user_id)
        assert self.is_admin()
        
    def get_clients_data(self):
        rows_registered = self.conn.select_all("""
                                              SELECT c.user_id, UNIX_TIMESTAMP(c.t) as t, m.email
                                                FROM algo_clients.clients c
                                               INNER JOIN algo_users.users u
                                                  ON u.id = c.user_id
                                                 AND u.deleted_t IS NULL
                                               INNER JOIN algo_users.map_email_to_user_id m
                                                  ON m.id = u.email_id
                                              """)
        
        rows_pending_register = self.conn.select_all("""
                                                       SELECT r.id, 
                                                              r.uuid, 
                                                              r.email, 
                                                              UNIX_TIMESTAMP(r.t) as t, 
                                                              sent_emails.cnt as sent_emails_cnt
                                                         FROM algo_clients.pending_register r
                                                        LEFT JOIN (
                                                            SELECT uuid, count(1) as cnt
                                                               FROM algo_clients.pending_register_sent_emails
                                                              GROUP BY uuid
                                                        ) sent_emails
                                                        ON sent_emails.uuid = r.uuid
                                                       """)
        
        return {
            'registered': rows_registered,
            'pending_register': rows_pending_register
        }
        
    def delete_client_pending_registration(self, uuid: str):
        self.conn.query("""
                          DELETE FROM algo_clients.pending_register
                           WHERE uuid = %s
                          """, [uuid])
        
        self.conn.query("""
                        DELETE FROM algo_clients.pending_register_sent_emails
                        WHERE uuid = %s
                        """, [uuid])

    def create_client_pending_registration(self, email: str):
        
        cnt = self.conn.select_one("""
                                             SELECT count(1) as cnt
                                               FROM algo_clients.pending_register
                                              WHERE email = %s
                                             """, [email])['cnt']
        
        if cnt > 0:
            return
        
        uuid = Utils.get_guid()
        
        self.conn.query("""
                          INSERT INTO algo_clients.pending_register (uuid, email)
                          VALUES (%s, %s)
                          """, [uuid, email])
        
    def send_client_pending_registration_email(self, uuid: str):
        
        email = self.conn.select_one("""
                                       SELECT email
                                         FROM algo_clients.pending_register
                                        WHERE uuid = %s
                                       """, [uuid])['email']
        
        assert ENV_DATA.client_url is not None
        
        subject = 'Confirm AiAlgo registration'
        body = f"""
            Please confirm AiAlgo registration by following this url:
            
            {ENV_DATA.client_url}/client?confirm-registration={uuid}
            """
        
        email_data = EmailSender().send(
            recipient=email,
            subject=subject,
            body=body
        )
        
        if email_data is not None:
            self.conn.query("""
                            INSERT INTO algo_clients.pending_register_sent_emails (uuid, email)
                            VALUES (%s, %s)
                            """, [uuid, json.dumps(email_data)])
        