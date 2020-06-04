
def determine_most_recent_date(files_list):
    # Expecting list of filenames in the format alma_bibs__2020050503_18470272620003811_new_1.xml
    return max([int(f.split("_")[3]) for f in files_list])