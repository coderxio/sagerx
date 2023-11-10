from airflow.decorators import task
import csv

@task
def modify_csv(file_path):
    print(f"Modifying CSV file at {file_path}")

    file = open(file_path)
    csvreader = csv.reader(file)

    for _ in range(3): 
         next(csvreader) # skips the first 3 rows

    rows = []
    for row in csvreader:
            rows.append(row)

    file.close()

    with open(file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)