from airflow.decorators import task
import csv

@task
def modify_csv(file_path):
    print(f"Modifying CSV file at {file_path}")

    with open(file_path, newline='') as file:
        csvreader = csv.reader(file)

        for _ in range(3): 
            next(csvreader,None) # skips the first 3 rows

        """
        Skip the top section by checking if the row is empty (signifying the end of the top section)

        The bottom section of each report contains all products in the Purple Book database for that month, 
        including the products listed in the top section that were added or changed.        
        """ 
        
        for row in csvreader:
            if not any(row):
                break

        rows = []
        for row in csvreader:
                rows.append(row)

    with open(file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)