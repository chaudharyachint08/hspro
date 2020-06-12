conda activate DSL
cd /mnt/sdb4/hspro

python -i source/main.py --new_info=True --do_plot=True --min_sel=0.001 --max_sel=1.0 --progression='AP' --qi=0
