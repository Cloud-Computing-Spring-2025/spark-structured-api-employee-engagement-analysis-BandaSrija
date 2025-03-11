from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def initialize_spark(app_name="Task2_Valued_No_Suggestions"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): The SparkSession object.
        file_path (str): Path to the employee_data.csv file.

    Returns:
        DataFrame: Spark DataFrame containing employee data.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_valued_no_suggestions(df):
    """
    Find employees who feel valued but have not provided suggestions and calculate their proportion.

    Parameters:
        df (DataFrame): Spark DataFrame containing employee data.

    Returns:
        tuple: Number of such employees and their proportion.
    """
    # Step 1: Identify employees with SatisfactionRating >= 4 (feeling valued).
    valued_employees_df = df.filter(col("SatisfactionRating") >= 4)
    
    # Step 2: Among these, filter those with ProvidedSuggestions == False (not provided suggestions).
    valued_no_suggestions_df = valued_employees_df.filter(col("ProvidedSuggestions") == False)
    
    # Step 3: Calculate the number of these employees.
    number_of_valued_no_suggestions = valued_no_suggestions_df.count()

    # Step 4: Calculate the total number of employees with SatisfactionRating >= 4.
    total_valued_employees = valued_employees_df.count()

    # Step 5: Calculate the proportion (percentage) of these employees.
    if total_valued_employees > 0:
        proportion = (number_of_valued_no_suggestions / total_valued_employees) * 100
    else:
        proportion = 0.0

    # Return both the number and proportion
    return number_of_valued_no_suggestions, round(proportion, 2)

def write_output(number, proportion, output_path):
    """
    Write the results to a CSV file along with the SUCCESS file.

    Parameters:
        number (int): Number of employees feeling valued without suggestions.
        proportion (float): Proportion of such employees.
        output_path (str): Path to save the output files.

    Returns:
        None
    """
    # Create a DataFrame for the results
    result_data = [(number, proportion)]
    result_df = spark.createDataFrame(result_data, ["Number of Employees", "Proportion"])

    # Write result to CSV (this will create part-00000, part-00001, etc.)
    result_df.coalesce(1).write.option("header", "true").csv(output_path, mode="overwrite")

    # Create a SUCCESS file manually (since it's not automatically done with CSV writes)
    success_file_path = f"{output_path}/_SUCCESS"
    with open(success_file_path, 'w') as success_file:
        success_file.write("SUCCESS")

def main():
    """
    Main function to execute Task 2.
    """
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-BandaSrija/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-BandaSrija/outputs/valued_no_suggestions.csv"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 2
    number, proportion = identify_valued_no_suggestions(df)
    
    # Write the result to CSV and create SUCCESS file
    write_output(number, proportion, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
