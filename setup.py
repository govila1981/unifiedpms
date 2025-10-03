"""
Setup configuration for Trade Processing Pipeline
Ensures all dependencies are properly installed
"""

from setuptools import setup, find_packages
import os

# Read the requirements file
def read_requirements():
    with open('requirements.txt', 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

# Read the README file
def read_readme():
    with open('README.md', 'r', encoding='utf-8') as f:
        return f.read()

setup(
    name='trade-processing-pipeline',
    version='2.0.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='Comprehensive trade processing system with strategy assignment and deliverables calculation',
    long_description=read_readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/trade-processing-pipeline',
    packages=find_packages(),
    install_requires=read_requirements(),
    python_requires='>=3.8,<4.0',
    include_package_data=True,
    package_data={
        '': ['*.csv', '*.xlsx', '*.md', '*.toml', '*.bat', '*.ps1'],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Financial and Insurance Industry',
        'Topic :: Office/Business :: Financial :: Investment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    entry_points={
        'console_scripts': [
            'trade-pipeline=launcher:main',
            'trade-pipeline-gui=launcher:StreamlitLauncher.run',
        ],
    },
)