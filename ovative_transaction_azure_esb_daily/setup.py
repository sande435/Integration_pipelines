from setuptools import setup, find_packages

setup(
    name='ovative_transaction_azure_esb_daily',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
        'google-cloud-storage',
        'google-api-core',
        'google-auth',
        'google-auth-oauthlib',
        'requests',
        'functions-framework',
        'google-api-python-client',
        'azure-core',       
        'azure-storage-blob==12.10.0',
        'snowflake',
        'snowflake-connector-python'
    ],
)



