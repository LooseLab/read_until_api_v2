read_until_api_v2
===

Python3 implementation of the read_until_api

---

Table of Contents
===
- [Features]()
- [Installation]()

Features
===

Setup and Installation
===

```bash
# Make a virtual environment
python3 -m venv venv3
source ./venv3/bin/activate

# Clone repos
git clone https://github.com/looselab/read_until_api_v2.git
git clone https://github.com/looselab/pyguppyplay.git
git clone https://github.com/looselab/ru.git

# Install
cd read_until_api_v2
pip install --upgrade pip -r requirements.txt
python setup.py develop

cd ../pyguppyplay
pip install -r requirements.txt
python setup.py develop

cd ../ru
pip install -r requirements.txt
python setup.py develop

```

TODO
===
- Use gRPC `set_analysis_configuration` with an edited JSON file to configure MK
