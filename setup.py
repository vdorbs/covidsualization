from setuptools import find_packages, setup

setup(
    name='covidsualization',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'us'
    ]
)
