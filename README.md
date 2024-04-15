# PySparkVerse

Welcome to the PySparkVerse repository! This repository contains a collection of PySpark code snippets and examples categorized by day for easy reference and learning. Whether you're new to PySpark or looking to deepen your understanding, you'll find a variety of topics covered here.

 ## Table of Contents
- [Day 1](#day-1)
<!--- [Day 2](#day-2)
- [Day 3](#day-3)
- [Day 4](#day-4)
- [Day 5](#day-5)
- [Day 6](#day-6)
- [Day 7](#day-7)t -->

## Day 1
<a href="https://github.com/am15398/PySparkVerse/blob/main/01_PySpark_Baisc/Day%201%20(PySpark).ipynb" target="_blank">Redirect to Day 1 notebook</a>
- Get count in DataFrame
- Select columns in DataFrame
- Filter Rows in DataFrame
- Avg of column & Alias in DataFrame
- Group by in DataFrame
- Order by in DataFrame
- Join in DataFrame
- Using Alias in join in DataFrame
- Union All in DataFrame
- Union in DataFrame
- Distinct in DataFrame
- DropDuplicates in DataFrame
- Limit in DataFrame
- New Column in DataFrame
- Filter on multiple column in DataFrame
- Subquery in DataFrame
- Between in DataFrame
- Like in DataFrame
- Case in DataFrame
- CountDistinct in DataFrame
- Substring in DataFrame
- Lit in DataFrame
- Concat column in DataFrame
- String concatenation in DataFrame
<!--
## Day 2
- Average over partition in DataFrame
- Sum over partition in DataFrame
- Lead function with default value in DataFrame
- Lag function with default value in DataFrame
- Drop column in DataFrame
- Rename column in DataFrame
- Change column datatype in DataFrame

## Day 3
- Creating table from DataFrame: Learn how to create a table from a DataFrame in PySpark.
- Insert data in DataFrame: Understand how to insert data into a DataFrame.
- Create table with specific column in DataFrame: Learn to create a table with specific columns from a DataFrame.
- Aggregate with alias in DataFrame: Perform aggregations with aliases in PySpark DataFrames.
- Nested subquery in DataFrame: Explore nested subqueries in PySpark DataFrames.
- Cross join in DataFrame: Learn how to perform cross joins between DataFrames.
- Group by having count greater than in DataFrame: Group by clauses with conditions in PySpark DataFrames.
- Alias for table join (default is inner) in DataFrame: Specify aliases for table joins in PySpark DataFrames.
- Select from multiple tables in DataFrame: Select data from multiple tables using PySpark DataFrames.

## Day 4
- Extract date part in DataFrame: Extract specific parts of a date from a DataFrame in PySpark.
- Inequality filtering in DataFrame: Filter DataFrame based on inequality conditions.
- In list in DataFrame: Filter DataFrame where a column value is in a list of values.
- Not in list in DataFrame: Filter DataFrame where a column value is not in a list of values.
- Null values in DataFrame: Handle null values in PySpark DataFrames.
- Not null values in DataFrame: Filter DataFrame to retrieve non-null values.
- Upper case in DataFrame: Convert column values to uppercase in PySpark DataFrames.
- Lower case in DataFrame: Convert column values to lowercase in PySpark DataFrames.
- Length in DataFrame: Calculate the length of strings in DataFrame columns.
- Trim case in DataFrame: Trim whitespace from the beginning and end of strings in DataFrame columns.
- Ltrim case in DataFrame: Trim whitespace from the beginning of strings in DataFrame columns.
- Rtrim case in DataFrame: Trim whitespace from the end of strings in DataFrame columns.
- String replace in DataFrame: Replace occurrences of a substring in DataFrame columns.
- Coalesce in DataFrame: Select the first non-null value from a list of columns in PySpark DataFrames.
- Date diff in DataFrame: Calculate the difference between two dates in PySpark DataFrames.
- Add months to date in DataFrame: Add a specified number of months to a date in PySpark DataFrames.

## Day 5
- First value in group in DataFrame: Get the first value in each group of a DataFrame.
- Last value in group in DataFrame: Get the last value in each group of a DataFrame.
- Row number over partition in DataFrame: Assign a unique row number to each row within a partition of a DataFrame.
- Rank number over partition in DataFrame: Assign a rank to each row within a partition of a DataFrame.
- Dense rank number over partition in DataFrame: Assign a dense rank to each row within a partition of a DataFrame.
- Min value in group in DataFrame: Find the minimum value in each group of a DataFrame.
- Min value in table in DataFrame: Find the minimum value in a DataFrame column.
- Max value in group in DataFrame: Find the maximum value in each group of a DataFrame.
- Max value in table in DataFrame: Find the maximum value in a DataFrame column.

## Day 6
- Left join in DataFrame: Perform a left join between two DataFrames in PySpark.
- Right join in DataFrame: Perform a right join between two DataFrames in PySpark.
- Outer join in DataFrame: Perform an outer join between two DataFrames in PySpark.
- Group by having in DataFrame: Filter groups using the HAVING clause in PySpark DataFrames.
- Round decimal value in DataFrame: Round decimal values in DataFrame columns to a specified number of decimal places.
- Today date in DataFrame: Retrieve the current date in PySpark DataFrames.
- Date addition in DataFrame: Add a specified number of days to a date in PySpark DataFrames.
- Date subtract in DataFrame: Subtract a specified number of days from a date in PySpark DataFrames.
- Year from date in DataFrame: Extract the year component from a date in PySpark DataFrames.
- Month from date in DataFrame: Extract the month component from a date in PySpark DataFrames.
- Day from date in DataFrame: Extract the day component from a date in PySpark DataFrames.
- Sorting in DataFrame: Sort DataFrame by one or more columns in ascending or descending order.
  
## Day 7
- dbutils.help(): Learn how to use dbutils.help() to get help on available functions in Databricks.
- dbutils.fs.help(): Explore dbutils.fs.help() to get help on file system operations in Databricks.
- Read files from folder: Read files from a folder into a DataFrame in PySpark.
- Today date: Retrieve the current date in PySpark DataFrames.
- Creating spark session: Learn how to create a SparkSession in PySpark.
- Read CSV file with header and schema: Read a CSV file with header and schema into a DataFrame.
- Read CSV file with skip 5 rows: Read a CSV file skipping the first 5 rows into a DataFrame.
- Dropping rows with missing value: Drop rows with missing values from a DataFrame.
- Fill null value: Fill null values in a DataFrame with specified values.
- Writing to parquet: Write DataFrame to Parquet file format.
- Broadcast join: Perform a broadcast join in PySpark.
- Get number of partitions: Get the number of partitions in a DataFrame.
- Increase the partition: Increase the number of partitions in a DataFrame.
- Decrease the partition: Decrease the number of partitions in a DataFrame.
- repartitionByRange: Repartition DataFrame by range into a specified number of partitions.
- Show the data: Display the contents of a DataFrame.
- Explain plan: Display the execution plan for a DataFrame.
- Read CSV file with permissive: Read a CSV file with permissive mode into a DataFrame.
- Read CSV file with DROPMALFORMED: Read a CSV file with DROPMALFORMED mode into a DataFrame.
- Read CSV file with FAILFAST: Read a CSV file with FAILFAST mode into a DataFrame.
- Read CSV file with permissive capture bad record: Read a CSV file with permissive mode and capture bad records into a DataFrame.
- Explode function (Array): Explode an array column into multiple rows in a DataFrame.
- Struct Field: Define struct fields in PySpark DataFrames.
- HASH (MD5): Calculate MD5 hash values for DataFrame columns.
- PySpark UDF: Define and use User Defined Functions (UDFs) in PySpark.
- Load data to delta table: Load data into a Delta table.
- Describe the detail of the table: Describe the schema of a table in PySpark.
- Get column details: Get details of columns in a PySpark DataFrame.
- Insert new row: Insert a new row into a DataFrame.
- Get the history of table: Get the history of changes made to a Delta table.
- Time travel feature: Use Delta's time travel feature to query historical versions of a table.
- Cache: Cache DataFrame for better performance.
- Analyze: Analyze DataFrame for better performance.
- Optimize the table: Optimize a Delta table for better performance.
- Optimize / Zorder: Z-order DataFrame for better query performance.
- Vacuum: Vacuum a Delta table to reclaim space by removing old versions of files.
- Read multiple files with filename in a new column: Read multiple files into a DataFrame and add filename as a new column. -->



## Usage
Each day's section contains a list of tasks along with the corresponding code snippets. You can simply navigate to the desired day to find the code examples you're interested in.

## Contribution
Contributions are welcome! If you have additional PySpark code snippets or improvements to existing ones, feel free to open a pull request.

## License
This repository is licensed under the [MIT License](LICENSE).
