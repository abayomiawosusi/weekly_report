from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in weekly_report/__init__.py
from weekly_report import __version__ as version

setup(
	name="weekly_report",
	version=version,
	description="Weekly Reports",
	author="abayomi.awosusi@sgatechsolutions.com",
	author_email="abayomi.awosusi@sgatechsolutions.com",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
