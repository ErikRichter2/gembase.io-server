from flask import request

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel


class PublicDbModel(BaseDbModel):
    
    def __init__(self, conn: DbConnection):
        super(PublicDbModel, self).__init__(conn=conn)
        
    def get_email_id(self, email: str):
        row = self.conn.select_one_or_none("""
            SELECT email_id
              FROM gb_public.emails
             WHERE email = %s
            """, [email])
        
        return row['email_id'] if row is not None else None

    def get_or_create_email_id(self, email: str):
        email_id = self.get_email_id(email=email)
        
        if email_id is None:
            email_id = self.conn.insert("""
                INSERT INTO gb_public.emails(email) VALUES (%s)
                """, [email])
        
        return email_id
    
    def get_or_create_registration_whitelist_pending(self, email: str):
        
        email_id = self.get_or_create_email_id(email=email)
        
        row = self.conn.select_one_or_none("""
            SELECT request_guid
              FROM gb_public.registration_whitelist_pending
             WHERE email_id = %s
            """, [email_id])

        if row is not None:
            return row["request_guid"]

        request_guid = Utils.get_guid()
        self.conn.query("""
            INSERT INTO gb_public.registration_whitelist_pending (email_id, ip_address, request_guid)
            VALUES (%s, %s, %s)
            """, [email_id, request.remote_addr, request_guid])

        return request_guid

    def get_or_create_unsubscribe_guid(self, email_id: int):            
        row = self.conn.select_one_or_none("""
                    SELECT request_guid 
                      FROM gb_public.email_subscription
                     WHERE email_id = %s
                    """, [email_id])
        
        if row is not None:
            return row["request_guid"]
        
        guid = Utils.get_guid()
        self.conn.query("""
                    INSERT INTO app.users_email_subscription (email_id, request_guid, subscribed)
                    VALUES (%s, %s, 1)
                    """, [email_id, guid])
        return guid
