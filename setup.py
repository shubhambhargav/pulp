from setuptools import setup, find_packages

setup(
        name="pulp",
        version="0.1.dev1",
        url=None,
        author="Shubham Bhargav",
        author_email="s.bhargav003@gmail.com",
        description=("Data Ingestion Framework"),
        packages=find_packages(),
        include_package_data=True,
        package_data={
                        "pulp.conf" : ["project_template/*-tpl", "app_template/*-tpl"]
        },
        scripts=["pulp/bin/pulp-admin.py"],
        entry_points={
                        "console_scripts": [
                                        "pulp-admin = pulp.core.management.executioner:execute_entry_point_from_cmd",
                            ]
        },
        install_requires=[
                            "requests",
                            "MySQL-python",
                            "luigi",
                            "elasticsearch",
                            "redis",
                            "kafka",
                            "boto"
                        ]
    )