from setuptools import setup, find_packages

setup(
    name="pyflow",
    version="0.1.0",
    description="A lightweight Python framework for building and running ETL data pipelines",
    author="Sam",
    packages=find_packages(exclude=["tests*", "examples*"]),
    python_requires=">=3.9",
    install_requires=[
        "pandas>=1.5",
        "requests>=2.28",
    ],
    extras_require={
        "postgres": ["sqlalchemy>=2.0", "psycopg2-binary>=2.9"],
        "dev": ["pytest>=7.0", "pytest-cov"],
    },
)
