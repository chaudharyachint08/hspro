import numpy as np, pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

def f(N,r=0.5):
	return (1-(1-r)**N)

X_pow = 6
for r in np.linspace(0.1,0.9,9):
	X = [2**n for n in range(X_pow)]
	Y = [f(x,r) for x in X]
	_ = plt.plot( X , Y , label='{:0.3f}'.format(r) )

plt.xscale('log',basex=2)
# plt.yscale('log',basey=2)

plt.xlabel('Step Size')
plt.ylabel('Exponential Weight')

title = 'Weight Calculation for Tuning Direction Vector in ESS Space'
plt.title(title)
plt.grid(True, which='both')
plt.legend()
plt.savefig( '{}.PNG'.format(title) , format='PNG' , dpi=400 , bbox_inches='tight' )
plt.show()
