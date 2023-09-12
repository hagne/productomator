import sys

required_verion = (3,)
if sys.version_info < required_verion:
    raise ValueError('ceilopy needs at least python {}! You are trying to install it under python {}'.format('.'.join(str(i) for i in required_verion), sys.version))

from setuptools import setup, find_packages

setup(
    name="productormator",
    version="0.1", #setting this caused a huge hadeache ... basically the script wasn't found when the version was set
    packages=find_packages(),
    author="Hagen Telg",
    author_email="hagen.telg@gmail.com",
    description="...",
    license="MIT",
    url="https://github.com/hagne/productormator",
    install_requires=[],
    # scripts=['scripts/cl51cloudprod',
    #          'scripts/cl51cloudprod_sailsplash'],
    # package_data={'': ['*.cdl']},
    # include_package_data=True,
    # zip_safe=False
)