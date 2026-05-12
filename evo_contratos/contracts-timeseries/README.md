# contracts-timeseries
Project to create master table for contract timeseries

## Setup Instructions

Follow these steps to set up the project environment.

### 1. Create and Activate Virtual Environment

**Windows (CMD):**
```
python -m venv .venv
.venv\Scripts\activate
```

**MacOS / Linux:**
```
python -m venv venv
source .venv/bin/activate
```

### 2. Install Dependencies

```
pip install -r requirements.txt
```

### 3. Run the Code

To run the main code

```
python main.py
```

## Create requirements.txt

To create a minimalist version of the requirements file, we recommend to use pipreqs (not on the virtual enviroment)

```
pipreqs . --print --ignore .venv
```
