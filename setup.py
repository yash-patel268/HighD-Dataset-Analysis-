from setuptools import setup

setup(
    name='my-dataflow-job',
    version='0.1',
    install_requires=[
        'pandas==1.2.4',
        'google-cloud-bigquery==2.30.0',
        'google-cloud-pubsub==2.8.0',
        'apache-beam[gcp]==2.33.0',
        'json'
    ]
)