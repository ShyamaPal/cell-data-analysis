# cell-data-analysis

In the mobile telephone network there are lots of devices that need to work together to give the customer a pleasant experience. Examples of those devices is a customers cell phone and so called cells which the phone connects to. Cells can have different features and have gone through evolution. There are cells of different technology gsm (2g), umts (3g) and lte (4g). Cells are grouped into physical locations called sites. One site has at least one cell and can be composed of any mix of cells in different technologies. At Telia we receive a snapshot of this cell/site data every day for all the countries.

Each cell of a technology can operate on different frequency bands. For gsm there are 900 and 1800 MHZ, umts has 900 and 2100 MHZ and lte has 700, 800, 2600, 1800 and 2100 MHZ.

## Problem Description
The task is to take the data in csv format and write a spark application based on DataFrames that analyses the data. The data has 2 parts. The first part is the cell data (gsm.csv, umts.csv and lte.csv) and the second one is the site data (site.csv). The data, as mentioned before, is organised by day. The task is to create one data set for all cells and enrich each cell with information from the site. All cells that cannot be assigned a site should be dropped.

The calculated values should be the following:

1. How many cells per technology are there on that site. The column name should be as follows: site_$TECHNOLOGY_cnt
     
     a. Example: for a site S there are 2 cells gsm, 3 cells umts and 0 cells lte. The result should be site_4g_cnt: 0, site_3g_cnt: 3, site_2g_cnt: 2

2. Which technology bands do exist on a site S
     
     a. Example: for a site S there is 2 cell gsm with 900MHZ, one cell umts 2100MHZ and one cell lte with 1800MHZ. The result should be: frequency_band_G900_by_site: 1devises, frequency_band_U2100: 1, frequency_band_L1800: 1, frequency_band_L2100: 0 (and so on)

3. The result is to be saved into some configurable directory. The granularity of the input data set is to be kept.

## Solution
### Setup environment
#### Prerequisites

**Dependencies**

* Install Python version 3.8
* git clone https://github.com/ShyamaPal/cell-data-analysis.git
* cd cell-data-analysis/
* Install pyspark and other dependencies from the Pipfile and create the virtual environment by running the below command.
```shell script
pipenv shell
pipenv install
```
### Run application
* Provide the input path as the parent of all the different datasets.
  Example: If the input datasets are present like `D:\Archive\gsm` provide `D:\Archive` as the input path, make sure all
  the datasets present in the same directory.
* Run the below command to write the output to the below mentioned output path.
```shell script
python -m cell_data_analysis --input-path <input path> --output-path <output path>
```
Example:
```shell script
python -m cell_data_analysis --input-path  D:\Archive\input --output-path D:\Archive\output
```
* After the execution is successful the resultant datasets will be stored in the below paths
  1. Cells per site: `<output_path>/cells_per_site` 
  1. Frequency per site: `<output_path>/frequency_per_site` 

### Run test
```
pytest test
```

### Deployment
The code can be deployed in the desired environment through Jenkins pipeline by building docker image with the python environment and running the test cases through pytest.
It can be scheduled to run in a dockerized way where any of the run commands can be executed.

### Configuration
The Spark configurations can be handled by running spark-submit command and passing the configurations dynamically.
Example: `spark-submit --conf <> --master <> cell_data_analysis/__main__.py`

### Improvement ideas
* use click instead of argparse and make it as an commandline
* improve test coverage
* dockerize the spark environment
