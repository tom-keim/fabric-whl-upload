"""Setup script for the hello-world Python package."""

from setuptools import find_packages, setup

setup(
    name="hello-world",
    version="0.1.1",
    description="A dummy Hello World Python package",
    author="Your Name",
    packages=find_packages(),
    install_requires=[],
    python_requires=">=3.6",
)
