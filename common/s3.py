import io
from typing import Optional

import minio
from minio import Minio


class S3Client:
    def __init__(self):
        # self.hook2 = Minio(
        #     "52.168.137.154:9001",
        #     access_key="w9pT75P6JZT8Br9e",
        #     secret_key="eSjErKCw9NdqE39L",
        #     secure=False
        # )
        # self.hook2 = Minio(
        #     "s3.dev.aragorn.xyz",
        #     access_key="F0U37GAHTTVVH499AKGO",
        #     secret_key="HCkcpiNcyumHIcATxEnfm1knVGehYP9zo4VyjgHP"
        # )

        self.hook2 = Minio(
            "s3.stage.aragorn.xyz",
            access_key="0T6IXB6UJHX7BSUR7A03",
            secret_key="5iQYT3HAzKrh5evb+KKgGu2Xg5mdTM8npo5BBVMx"
        )

    def load_data(
            self,
            data: any,
            key: str,
            bucket_name: Optional[str] = None,
            is_string: bool = True,
    ) -> None:
        if is_string:
            data = data.encode('utf-8')

        file_obj = io.BytesIO(data)
        res = self.hook2.put_object(bucket_name, key, file_obj, len(data))

        file_obj.close()

        return res

    def read_key(self, key: str, bucket_name: Optional[str] = None) -> str:
        res = self.hook2.get_object(bucket_name, key).data.decode('utf-8')
        return res

    def check_for_key(self, wildcard_key: str, bucket_name: Optional[str] = None) -> bool:
        try:
            self.hook2.stat_object(bucket_name, wildcard_key)
            return True
        except minio.error.S3Error:
            return False
