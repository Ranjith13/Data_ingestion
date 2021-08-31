import os

CREDENTIALS = {
    'source': {
        'source_type': 'AWS_s3',
        'FILE_NAME': 'sample_data.csv',
        'BUCKET_NAME': os.getenv('BUCKET_NAME'),
        'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
        'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY')

    },
    'TARGET_DB': {
        'DB_TYPE': 'mysql',
        'DB_HOST': 'localhost',
        'DB_NAME': 'CreditRisk_DB',
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASS': os.getenv('DB_PASS')
    }
}
