from setuptools import setup, find_packages

setup(
    name='ecommercetool-common',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
        'google-cloud-storage',
        'paramiko',
        'google-cloud-pubsub',
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
