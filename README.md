# IATI Humanitarian Data Portal - data

This repository generates the data for the [IATI Humanitarian Data Portal](https://humportal.org).

Data is generated daily and pushed to Github Pages.

## Installation

Clone this repository:
```bash
git clone git@github.com:markbrough/humportal-data.git
cd humportal-data
```

Set up a `virtualenv` and install requirements:
```bash
virtualenv ./pyenv -p python3
source ./pyenv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python run.py
```
