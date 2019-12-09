from setuptools import setup
import versioneer

requirements = [
    'requests'
]

setup(
    name='prism',
    version=versioneer.get_version(),
    description="Python API client to load data into Prism.",
    author="Curtis Hampton",
    author_email='CurtLHampton@gmail.com',
    url='https://github.com/Workday/prism-python',
    packages=['prism'],
    install_requires=requirements,
    extras_require={
        'dev': [
            'pytest',
        ]
    },
    keywords='prism',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
