from app import app


@app.get('/uptime-check')
def get_api_uptime_check():
    return "OK", 200
