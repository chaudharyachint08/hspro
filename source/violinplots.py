import numpy as np, pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

np.random.seed(42)

data = []
for IC_ix in range(np.random.randint(3,10)):
	for point_ix in range(np.random.randint(25,100)):
		data.append([IC_ix,np.random.random()])

# np.random.shuffle(data)
data = np.array(data).T

df = pd.DataFrame({'Deviation':data[1],'Contour Index':data[0]})

# sns.violinplot( x='Contour Index', y='Deviation', hue=None, data=df, gridsize=100, inner='quartile' )
sns.violinplot( x='Contour Index', y='Deviation', hue=None, data=df, gridsize=100 )

plt.show()