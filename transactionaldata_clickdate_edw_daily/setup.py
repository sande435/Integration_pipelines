from setuptools import setup, find_packages

setup(
    name='transactionaldata_clickdate_adhoc_cj_edw_daily',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
        'google-cloud-storage',
        'google-api-core',
        'google-auth',
        'google-auth-oauthlib',
        'google-cloud-pubsub',
        'requests',
        'functions-framework',
        'google-api-python-client',
        'google-cloud-secret-manager'
    ]
)
        
