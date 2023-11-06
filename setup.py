from setuptools import setup
import versioneer

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="prism",
    version=versioneer.get_version(),
    description="Python API client to load data into Prism.",
    author="Curtis Hampton, Mark Waldron, Jacinta Corbett, Mark Greynolds",
    author_email="CurtLHampton@gmail.com",
    url="https://github.com/Workday/prism-python",
    packages=["prism", "prism.commands"],
    package_data={"prism": ["data/*"]},
    entry_points={"console_scripts": ["prism=prism.cli:cli"]},
    install_requires=requirements,
    extras_require={"dev": ["pytest"]},
    keywords="prism",
    classifiers=["Programming Language :: Python :: 3.9"],
)
