from setuptools import setup

requirements = [
    'requests'
]

setup(
    name='prism',
    version='0.1.0',
    description="Python API client to load data into Prism.",
    author="Curtis Hampton",
    author_email='CurtLHampton@gmail.com',
    url='https://github.com/Workday/prism-python',
    packages=['prism'],
    install_requires=requirements,
    keywords='prism',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
