import requests

from common.pgsql import get_connection
from common.workplace import ExportData

SELECT_ALL_WORKPLACE_POST = """SELECT workplace_id,id FROM post WHERE workplace_id is not Null"""

SELECT_USER_BY_WORKPLACE_ID = """SELECT id
                                FROM users t
                                WHERE workplace_id = %s
                                LIMIT 1"""

APPEND_REACTION_TO_POST = """INSERT INTO post_like (user_id,post_id)VALUES (%s,%s) on conflict do nothing"""


class ExportPostsReactions(ExportData):
    def __init__(self, names, tokens):
        super().__init__(names, tokens)

        self.conn = get_connection()

    def process(self, i):
        params = self.ACCESS.get(f'{i}')
        with self.conn.cursor() as cursor:
            cursor.itersize = 20
            cursor.execute(SELECT_ALL_WORKPLACE_POST)
            i = 0
            for row in cursor:
                i = i + 1
                self.get_reactions(params, row[0], row[1])

    def get_reactions(self, params, wp_post_id, intranet_post_uuid):
        url = self.BASE_URL + f'{wp_post_id}/reactions'
        r = requests.get(url, params=params)
        print('первая страница')
        if r.json().get('data') is not None:
            self.insert_reactions(r.json().get('data'), intranet_post_uuid)

        while r.json().get('paging') is not None:
            try:
                r = requests.get(r.json().get('paging').get('next'), params=params)
                print('след страница')
                if r.json().get('data') is not None:
                    self.insert_reactions(r.json().get('data'), intranet_post_uuid)
            except requests.exceptions.MissingSchema:
                print('страницы кончились')
                break

    def insert_reactions(self, reactions, post_uuid):
        with self.conn.cursor() as cursor:
            for reaction in reactions:
                cursor.execute(SELECT_USER_BY_WORKPLACE_ID, (reaction.get('id'),))
                user_uuid = cursor.fetchone()

                if user_uuid is not None:
                    cursor.execute(APPEND_REACTION_TO_POST, (user_uuid[0], post_uuid))

            self.conn.commit()
            cursor.close()
