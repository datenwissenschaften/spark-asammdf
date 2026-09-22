import os
import sys
from setuptools import setup, find_packages, Extension

try:
    from Cython.Build import cythonize
except ImportError:
    # Fallback if Cython is not installed
    def cythonize(extensions, **kwargs):
        return extensions

extensions = [
    Extension("mdf_spark.helper", ["src/main/python/mdf_spark/helper.py"]),
    Extension("mdf_spark.bridge", ["src/main/python/mdf_spark/bridge.py"]),
]

from setuptools.command.build_py import build_py

class build_py_no_source(build_py):
    def build_packages(self):
        super().build_packages()
        # Remove .py files from build directory for packages we cythonized
        for package in self.packages:
            if package == "mdf_spark":
                package_dir = os.path.join(self.build_lib, package)
                for f in os.listdir(package_dir):
                    if f.endswith(".py") and f != "__init__.py":
                        os.remove(os.path.join(package_dir, f))

setup(
    name="mdf-spark",
    version="1.0.0",
    package_dir={"": "src/main/python"},
    packages=find_packages(where="src/main/python"),
    cmdclass={
        'build_py': build_py_no_source,
    },
    ext_modules=cythonize(extensions, compiler_directives={'language_level': "3"}),
    install_requires=[
        "pyspark>=4.0.0",
        "asammdf",
        "pyarrow",
        "pandas",
        "numpy",
    ],
    zip_safe=False,
)
