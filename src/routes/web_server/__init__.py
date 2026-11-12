from app import app


@app.post('/commands/public')
def commands_public():
    from src.routes.web_server.commands.public_commands import public_commands_handler
    return public_commands_handler.process_request()
  
    
@app.post('/commands/users')
def commands_users():
    from src.routes.web_server.commands.users_commands import users_commands_handler
    return users_commands_handler.process_request()


@app.post('/commands/portal')
def commands_portal():
    from src.routes.web_server.commands.portal_commands import portal_commands_handler
    return portal_commands_handler.process_request()


@app.post('/commands/calc')
def commands_calc():
    from src.routes.web_server.commands.calc_commands import calc_commands_handler
    return calc_commands_handler.process_request()


@app.post('/commands/billing')
def commands_billing():
    from src.routes.web_server.commands.billing_commands import billing_commands_handler
    return billing_commands_handler.process_request()


@app.post('/commands/studies')
def commands_studies():
    from src.routes.web_server.commands.studies_commands import studies_commands_handler
    return studies_commands_handler.process_request()


@app.post('/commands/public/survey')
def commands_public_survey():
    from src.routes.web_server.commands.public_survey_commands import public_survey_commands_handler
    return public_survey_commands_handler.process_request()


@app.post('/commands/user/survey')
def commands_user_survey():
    from src.routes.web_server.commands.user_survey_commands import user_survey_commands_handler
    return user_survey_commands_handler.process_request()
