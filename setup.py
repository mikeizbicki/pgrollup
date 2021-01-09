import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pg_rollup",
    version="0.0.1",
    author="Mike Izbicki",
    author_email="mike@izbicki.me",
    description="A package for generating rollup tables in postgresql",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mikeizbicki/pg_rollup",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
