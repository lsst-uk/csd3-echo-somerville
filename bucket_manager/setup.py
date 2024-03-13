from setuptools import setup, find_packages

setup(
    name='bucket_manager',
    version='0.1',
    packages=find_packages(),
    description='Helper functions for using an s3 bucket',
    author='Dave McKay',
    author_email='d.mckay@epcc.ed.ac.uk',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='s3 bucket management cephs3 swift',
    install_requires=['boto3', 'python-swiftclient'],
)