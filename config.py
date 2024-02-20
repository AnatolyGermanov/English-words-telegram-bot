import os

# Bot and server config
API_TOKEN = os.environ.get('API_TOKEN')
APP_HOST = os.environ.get('APP_HOST')
APP_PORT = os.environ.get('APP_PORT')
WEB_HOOK_URL = os.environ.get('WEB_HOOK_URL')
TELEGRAM_API_URL = f'https://api.telegram.org/bot{API_TOKEN}'

# Database config
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')
