import setuptools
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='mse',
    packages=['mse'],
    version='0.1.2',
    license='Apache license 2.0',
    description='Make Structs Easy (MSE)',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='fqaiser94',
    author_email='',
    url='https://github.com/fqaiser94/mse',
    keywords=['pyspark', 'struct', 'StructType', 'add', 'drop', 'rename'],
    install_requires=['pyspark>=2.4'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ]
)
