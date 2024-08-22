from airflow.decorators import task
from dailymed.dailymed import DailyMed
from dailymed.dailymed_images import DailyMedImages

@task 
def unzip_data(data_folder):
    dm = DailyMed(data_folder)
    dm.unzip_data()
    

@task
def process_dailymed(data_folder):
    dm = DailyMed(data_folder)
    dm.map_files()
    dm.extract_and_upload_dmd_base_data()


@task
def process_dailymed_images(data_folder): 
    dmi = DailyMedImages(data_folder)
    dmi.map_files()
    print("Completed Mapping")
    dmi.extract_and_upload_mapped_ndcs_from_image_files()
    print("Image Files Complete")
    dmi.extract_and_upload_mapped_ndcs_from_image_components()
    print("Image Components Complete")
    # dmi.extract_and_upload_mapped_ndcs_from_image_barcode()
    # print("Barcode Reading complete")
    #dmi.extract_and_upload_mapped_ndcs_from_image_ocr()