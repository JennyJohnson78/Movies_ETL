# Movies ETL

## Overview
Create an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables. Refactoring of code is a core data analytics concept and key to creating one function that takes in three files—Wikipedia data, Kaggle metadata, and MovieLens rating data—and performs the ETL process by adding the data to a PostgreSQL database.

Steps to technical analysis:
- Write an ETL Function to Read Three Data Files
- Extract and Transform the Wikipedia Data
- Extract and Transform the Kaggle data
- Create the Movie Database

## Results

### Write an ETL Function to Read Three Data Files

- Create a function that takes in three arguments

```
def extract_transform_load():
    
    # Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}ratings.csv')

    # Open the read the Wikipedia data JSON file.
    with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file: 
        wiki_movies_raw = json.load(file)
    
    # Read in the raw wiki movie data as a Pandas DataFrame.
    wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    # Return the three DataFrames
    return wiki_movies_df, kaggle_metadata, ratings
```

- Create the path to file directory and variables for the three files

```
# Path
file_dir = "C:/Users/JennyPC/OneDrive/Desktop/MSU/hw/08/Movies_ETL/"
# Wikipedia data
wiki_file = f'{file_dir}/wikipedia.movies.json'
# Kaggle metadata
kaggle_file = f'{file_dir}/movies_metadata.csv'
# MovieLens rating data.
ratings_file = f'{file_dir}/ratings.csv'
```

- Set the three variables equal to the function created in Step 1
```
wiki_file, kaggle_file, ratings_file = extract_transform_load()
```

- Set the DataFrames from the return statement equal to the file names
```
wiki_movies_df = wiki_file
kaggle_metadata = kaggle_file
ratings = ratings_file
```

Sample DataFrame:
![image](https://user-images.githubusercontent.com/67409852/152742511-f8310f0e-dc18-4ffe-9ef5-629a105c89a9.png)

### Extract and Transform the Wikipedia Data

### Extract and Transform the Kaggle data

### Create the Movie Database

## Summary
