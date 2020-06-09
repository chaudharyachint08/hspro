import numpy as np, pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

# np.random.seed(42)

deviation_dict = {}
for IC_ix in range(np.random.randint(3,10)):
	deviation_dict[IC_ix] = []
	for point_ix in range(np.random.randint(25,100)):
		deviation_dict[IC_ix].append( np.random.random() )


IC_ix_ls        = [ IC_ix      for IC_ix in deviation_dict for deviation in deviation_dict[IC_ix]]
deviation_ix_ls = [ deviation  for IC_ix in deviation_dict for deviation in deviation_dict[IC_ix]]

df = pd.DataFrame({'Cost Deviation':deviation_ix_ls,'Contour Index':IC_ix_ls})

# sns.violinplot( x='Contour Index', y='Deviation', hue=None, data=df, gridsize=100, inner='quartile' )
sns.violinplot( x='Contour Index', y='Cost Deviation', hue=None, data=df, gridsize=100 )
plt.grid(True)

plt.show()