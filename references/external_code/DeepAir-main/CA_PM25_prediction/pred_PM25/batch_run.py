import datetime
import os

"""
Generate results for 10 different models for the years 2014-2017.
Minimized the generation of intermediate data to reduce disk read/write processes.
Need to check the size of data generated each time; it seems /data might not have enough storage for all this data.
"""

used_group_list = [5, 9, 0, 1, 2, 3, 4, 6, 7]
year_list = [2014, 2015, 2016, 2017]
for used_group in used_group_list:
    for year in year_list:
        # mergeData and pred PM2.5
        predCA_PM_cmd = f"python daily_CA_PM25_mergeGBMPred_batch.py --data_dir ElevaLand.preTrain.waterDropLet --kind preProcess --year {year} --used_group {used_group} --CNNembed_dirs CA_preTrain.ElevaLand.embedResults"
        print(f"\nRunning cmd is : {predCA_PM_cmd}")
        os.system(predCA_PM_cmd)
        # plot everyday PM2.5
        firstDayInYear = datetime.datetime.strptime(str(year) + "-01-01", "%Y-%m-%d")
        lastDayInYear = datetime.datetime.strptime(str(year) + "-12-31", "%Y-%m-%d")
        numDaysInYear = (lastDayInYear - firstDayInYear).days + 1

        for d in range(0, numDaysInYear):
            yearDay = str(year) + str(d).zfill(3)
            # plot PM2.5
            plot_CAgrid_cmd = f"python daily_plotCA_PM25.py --save_dirs ElevaLand.preTrain.waterDropLet --year {year} --yearDay {yearDay} --save_fig --used_group {used_group}"
            print(f"\nRunning cmd is : {plot_CAgrid_cmd}")
            os.system(plot_CAgrid_cmd)
