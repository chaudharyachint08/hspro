conda activate DSL
cd /mnt/sdb4/hspro

python -i source/main.py --new_info=True --do_plot=True --nexus_tolerance=0.05 --bisection_lambda=0.0 --ada_momentum=0.8 --min_sel=0.001 --max_sel=1.0 --adaexplore=False --progression='GP' --qi=0
