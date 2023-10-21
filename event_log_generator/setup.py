from setuptools import setup, find_packages

setup(
    name='event_reader',
    version='0.1.5',
    description='A package to read events from a database using SQLAlchemy and Pandas',
    author='Ben Lakhoune',
    author_email='a.b.lakhoune@gmail.com',
    packages=find_packages(),
    install_requires=['sqlalchemy', 'pandas', 'dogpile.cache'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    py_modules=['db_utils'], 
    long_description='A package to read events from a database using SQLAlchemy and Pandas'
)
