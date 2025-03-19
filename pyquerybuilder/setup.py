# setup.py
from setuptools import setup, find_packages

setup(
    name="pyquerybuilder",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "snowflake-connector-python>=2.7.0",
    ],
    description="Fluent SQL query builder with schema discovery",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/pyquerybuilder",
)