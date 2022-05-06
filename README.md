# SeeDb

## Running the Project

### Installing from Git
```
$ git clone https://github.com/Divya-Maiya/SeeDb.git
$ cd SeeDb
```

### Installing from Zip folder
Unzip the project folder by right clicking on the zipped folder and selecting extract all

### Assumptions made
* We are assuming that PostgreSQL is preinstalled and the login crentials are known. 
* We also assume that `python` version `3.7.1` (or above) and `pip` are preinstalled. If not, please install python and follow the instructions here to install pip: https://pip.pypa.io/en/stable/installation/

### Downloading the required libraries/packages
All the required libraries are the `requirements.txt` file. To install all the libraries to run the project, run the following command. 
```
$ pip install -r requirements.txt
```

To set the credentials for PostgreSQL, please navigate to `SeeDb/config/seedb_configs.ini` and enter the correct details for the following:
* seedb_database_census = \<enter postgres db to connect to for setting up the census tables>
* seedb_database_dblp = \<enter postgres db to connect to for setting up the dblp tables>
* seedb_user = \<enter postgres username>
* seedb_password = \<enter postgres password>
* basepath = /\<path to SeeDb>/SeeDb

**Note:** Please ensure that the values you provide for seedb_database_census and seedb_database_dblp are names of actual databases that exist in your local postgres instance. (Both of these values could point to the same database on your machine)

### Using command line arguments
The following list explains the acceptable command line arguments: 
1. kl_divergence (Kullback-Leibler divergence)
2. emd_distance (Earth Mover's Distance)
3. js_divergence_distance (Jensen-Shannon divergence)
4. euclidean_distance
5. no argument defaults to kl_divergence

These command line arguments are to take user input for the utility measure that needs to be used. Please refer the section below to see an example on how to use the above.

Further, any other command line arguments not listed above will default to 'kl_divergence'. Also, when no command line argument is given, 'kl_divergence' will be taken as default.

### Running the project from the command line
Please make sure you are inside the `/src` folder:
```commandline
$ cd src
```
To run the project using the Census dataset and KL Divergence, please refer to the following: 
```commandline
$ python main_census.py kl_divergence
```

To run the project using the DBLP dataset, please refer to the following: 
```commandline
$ python main_dblp.py kl_divergence
```

To run the project using different distances for dblp and census datasets, please refer to the following: 
```commandline
$ python main_census.py emd_distance
$ python main_dblp.py emd_distance
$ python main_census.py js_divergence_distance
$ python main_dblp.py js_divergence_distance
$ python main_census.py euclidean_distance
$ python main_dblp.py euclidean_distance

```

The above command will execute the project using DBLP Dataset and KL Divergence as a utility distance.

### Alternative: Using ipynb
An alternative to running the project on the command line is the ipynb notebook.
* For the Census Dataset: This is present in `SeeDb/src/Results_Census.ipynb`
* For the DBLP Dataset: This is present in `SeeDb/src/Results_Dblp.ipynb`
