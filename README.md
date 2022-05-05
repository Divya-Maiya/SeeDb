# SeeDb

## Running the Project

### Installing from Git
```
$ git clone https://github.com/Divya-Maiya/SeeDb.git
$ cd SeeDb
```

### Assumptions made
* We are assuming that PostgreSQL is preinstalled and the login crentials are known. 
* We also assume that `python` and `pip` are preinstalled. If not, please install python and follow the instructions here to install pip: https://pip.pypa.io/en/stable/installation/

### Downloading the required libraries/packages
All the required libraries are the `requirements.txt` file. To install all the libraries to run the project, run the following command. 
```
$ pip install -r requirements.txt
```

### Using command line arguments
The following list explains the acceptable command line arguments: 
1. kl_divergence (Kullback-Leibler divergence)
2. emd_distance (Earth Mover's Distance)
3. js_divergence_distance (Jensen-Shannon divergence)
4. euclidean_distance

These command line arguments are to take user input for the utility measure that needs to be used. Please refer the section below to see an example on how to use the above.

Further, any other command line arguments not listed above will default to 'kl_divergence'. Also, when no command line argument is given, 'kl_divergence' will be taken as default.

### Running the project from the command line
Please make sure you are insied the `/src` folder:
```commandline
$ cd src
```
To run the project using the Census dataset and EMD distance, please refer to the following: 
```commandline
$ python main_census.py emd_distance
```

To run the project using the DBLP dataset, please refer to the following: 
```commandline
$ python main_dblp.py 
```

The above command will execute the project using DBLP Dataset and KL Divergence as a utility result.

### Alternative: Using ipynb
Alternative to running the project on the command line, the ipynb notebook.
* For the Census Dataset: This is present in `SeeDb/src/Results_Census.ipynb`
* For the DBLP Dataset: This is present in `SeeDb/src/Results_Dblp.ipynb`
## Folder Structure
