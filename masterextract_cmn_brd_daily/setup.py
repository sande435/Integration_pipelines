from setuptools import setup, find_packages

setup(
    name='masterextract-cmn-brd-daily',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
        'google-cloud-storage',
        'oracledb',
        'pandas',
        'functions-framework',
        'google-api-python-client',
        'google-auth',
        'flask',
        'google-api-core',
        'google-auth-oauthlib',
        'requests',
        'google-cloud-secret-manager'
    ],
)

