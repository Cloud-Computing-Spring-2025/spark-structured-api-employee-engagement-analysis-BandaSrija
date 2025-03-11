# task1_departments_satisfaction_analysis.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, format_string

def create_spark_session(app_name="Department_Satisfaction_Analysis"):
    """
    Create and return a Spark session.
    """
    session = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
    return session

def read_data(session, file_path):
    """
    Load employee data from the given CSV file into a Spark DataFrame.
    
    Parameters:
        session (SparkSession): The SparkSession object.
        file_path (str): The path to the CSV file.
    
    Returns:
        DataFrame: The loaded Spark DataFrame.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    data_frame = session.read.csv(file_path, header=True, schema=schema)
    return data_frame

def analyze_department_satisfaction(data_frame):
    """
    Analyze departments where more than 5% of employees have a high satisfaction rating and engagement.
    
    Parameters:
        data_frame (DataFrame): The Spark DataFrame with employee data.
    
    Returns:
        DataFrame: A DataFrame showing departments with more than 5% high satisfaction employees.
    """
    # Filter employees with a high satisfaction rating and engagement level
    high_satisfaction_df = data_frame.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))
    
    # Count employees with high satisfaction by department
    department_high_satisfaction = high_satisfaction_df.groupBy("Department").agg(count("*").alias("HighSatisfactionCount"))
    
    # Count total employees by department
    department_total_count = data_frame.groupBy("Department").agg(count("*").alias("TotalCount"))

    # Calculate percentage of high satisfaction employees
    department_percentage = department_high_satisfaction.join(department_total_count, "Department")\
                                                       .withColumn("Percentage", format_string("%.2f%%", (col("HighSatisfactionCount") / col("TotalCount") * 100)))\
                                                       .filter(col("Percentage").substr(0, 4) > "5.00")
    
    return department_percentage.select("Department", "Percentage")

def save_output(data_frame, output_path):
    """
    Save the result DataFrame to a CSV file.
    
    Parameters:
        data_frame (DataFrame): The DataFrame to save.
        output_path (str): Path to save the output file.
    """
    data_frame.coalesce(1).write.option("header", "true").csv(output_path, mode='overwrite')

def main():
    """
    Main function to execute the satisfaction analysis for departments.
    """
    # Create a Spark session
    session = create_spark_session()

    # Define input and output file paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-BandaSrija/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-BandaSrija/outputs/high_satisfaction_departments.csv"
    
    # Load the data from the CSV file
    employee_data = read_data(session, input_file)
    
    # Perform the department analysis
    result = analyze_department_satisfaction(employee_data)
    
    # Save the result to a CSV file
    save_output(result, output_file)

    # Stop the Spark session
    session.stop()

# Ensure the script runs only if executed directly
if __name__ == "__main__":
    main()
