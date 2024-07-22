import sys
import os
import warnings

PY_VERSION = "3.11.9"


def check_env() -> None:
    # get the conda virtual environment
    conda_vir = os.path.basename(os.environ.get('CONDA_PREFIX')).replace('_', '').lower()
    # get the pycharm proj name by working dir
    pycharm_proj = os.path.basename(os.getcwd()).lower()

    if conda_vir != pycharm_proj:
        warnings.warn("The conda virtual environment does not match with the Pycharm proj!")
    # get python version
    if sys.version.split('|')[0].strip() != PY_VERSION:
        warnings.warn("The Pyhon version is not " + PY_VERSION)
    print("Done!")


if __name__ == "__main__":
    check_env()



