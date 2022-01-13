import psycopg2
#
# DB_CONFIG = {
#     'dbname': 'social_dev',
#     'user': 'social_dev_user',
#     'password': 'egha7wa4Earah3xoog',
#     'host': '127.0.0.1',
#     'port': '5433',
# }

# DB_CONFIG = {
#     'dbname': 'social',
#     'user': 'oyanezie',
#     'password': 'b2nQUMmWGkk2wr7D',
#     'host': '127.0.0.1',
#     'port': '5438',
# }

DB_CONFIG = {
    'dbname': 'social',
    'user': 'oyanezie',
    'password': 'b2nQUMmWGkk2wr7D',
    'host': '127.0.0.1',
    'port': '5433',
}

keepalive_kwargs = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

def get_connection():
    try:
        conn = psycopg2.connect(port=DB_CONFIG.get('port'), dbname=DB_CONFIG.get('dbname'), user=DB_CONFIG.get('user'),
                                password=DB_CONFIG.get('password'), host=DB_CONFIG.get('host'),
                                **keepalive_kwargs)
        return conn
    except Exception as e:
        print(e)
