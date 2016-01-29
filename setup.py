from setuptools import setup

setup(
    name='sqltxt',
    version='1.0',
    install_requires=[
        'pyparsing',
        'ordered-set',
        'docopt'
    ],
    entry_points={
        'console_scripts': [
            'sqltxt = sqltxt.__main__:main'
        ]
    },
    extras_require={
        'test': ['pytest'],
    },
)
