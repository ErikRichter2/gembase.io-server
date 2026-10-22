from app import app

from src.routes.web_server.commands.public_commands import public_commands_handler
from src.routes.web_server.commands.users_commands import users_commands_handler


@app.post('/commands/public')
def commands_public():
  return public_commands_handler.process_request()
  
    
@app.post('/commands/users')
def commands_users():
  return users_commands_handler.process_request()
