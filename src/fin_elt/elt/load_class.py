import boto3


class Load:

    @staticmethod
    def to_parquet_s3(
            df,
            target_path,
            # AWS CREDS
    ):
        pass
        # Need watermark, only save latest day: append to file?
        # Need initial load plus incremental load
        # Is SQL Jinja even possible here?
        # https://stackoverflow.com/questions/47113813/using-pyarrow-how-do-you-append-to-parquet-file
        # Upsert to Pandas DF?