import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--year", type=str)
parser.add_argument("--remove_file_name", type=str)
parser.add_argument("--save_dir", type=str)
parser.add_argument("--used_group", type=str)
args = parser.parse_args()

if __name__ == "__main__":
    year = args.year
    save_dir = args.save_dir
    used_group = args.used_group

    del_cmd = f"find ./{save_dir}/G{used_group}/{year}/ -name {args.remove_file_name}.csv | xargs rm -rf"
    os.system(del_cmd)
    print("Bingo")


"""
python batch_remove_tempData.py --year 2016 --remove_file_name merged_CA_data_waterDropLet --save_dir CA_ElevaLand.preTrain.shapeNoDrop.waterDropLet.backUp
"""
