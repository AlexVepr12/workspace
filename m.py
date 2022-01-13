import os
import uuid
from urllib.parse import urlparse

import requests

from common.s3 import S3Client

client = S3Client()

file_url = 'https://video-arn2-1.xx.fbcdn.net/v/t42.1790-2/10000000_256404009213424_8592976511587324785_n.mp4?_nc_cat=103&ccb=1-5&_nc_sid=985c63&efg=eyJybHIiOjMwMCwicmxhIjoyNDA4LCJ2ZW5jb2RlX3RhZyI6InN2ZV9zZCJ9&_nc_ohc=j5ma1JorW_UAX-Ts2jP&_nc_oc=AQlsc-jq3fjxMbY0JG0H2vRIa2Rjw5yfPEm7CvwHh3QO-eK7b5YLsVI6GxKWPrsKuas&rl=300&vabr=59&_nc_ht=video-arn2-1.xx&edm=AHVUxfIEAAAA&oh=0aece163212757e9a80b467f89f75d2c&oe=6179F048'

try:
    print(file_url)
    r = requests.get(file_url)
    ext = os.path.splitext(os.path.basename(urlparse(file_url).path))[1]
    print(ext)
    avatar_name = f'workplace/attachments/' + str(uuid.uuid4()) + ext
    print(avatar_name)

    client.load_data(r.content, avatar_name, "social-files", False)
except Exception as e:
    print(e)
