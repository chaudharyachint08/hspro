'''
======================
3D surface (color map)
======================

Demonstrates plotting a 3D surface colored with the coolwarm color map.
The surface is made opaque by using antialiased=False.

Also demonstrates using the LinearLocator and custom formatting for the
z axis tick labels.
'''

# This import registers the 3D projection, but is otherwise unused.
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 unused import

import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
import numpy as np

plt.rcParams.update({'font.size': 22})

def log(val, base=np.e):
	return np.log(val)/np.log(base)

r_pb = 2
c_min, c_max_old, c_max_new = 20, 200, 1000

m_old, m_new = int(np.floor( log(c_max_old/c_min , r_pb)+1 )), int(np.floor( log(c_max_new/c_min , r_pb)+1 ))

x_delta, y_delta = 0.5, 0.7
x_min, x_max_old = 0, 1.0
x_max_new = x_max_old + x_delta


plt.axvline( x_max_old ,color='k',linestyle='-')


cmin_handle = plt.axhline( log(c_min, r_pb) ,xmin=x_min,xmax=x_max_new,color='y',linestyle='--')

# Plotting contours of old Plan bouquet
for i in range(m_old):
	old_handle = plt.axhline( log(c_max_old/r_pb**i,r_pb) ,xmin=x_min,xmax=x_max_old/x_max_new,color='b',linestyle='--')

# Plotting contours of new Plan bouquet
for i in range(m_new):
	new_handle = plt.axhline( log(c_max_new/r_pb**i,r_pb) ,xmin=x_min,xmax=x_max_new/x_max_new,color='r',linestyle=':')

val = log(c_max_new/c_max_old,r_pb)
if abs(val-int(val))<1e-6: # not a multiple, plot last red contour
	m_extra = m_new - m_old - 1
else:
	m_extra = m_new - m_old
	last_handle = plt.axhline( log(c_max_new,r_pb) ,xmin=x_min,xmax=x_max_new/x_max_new,color='k',linestyle='--')

# Plotting contours of extension of old bouquet
for i in range(m_extra):
	ext_handle = plt.axhline( log(c_max_old*r_pb**(i+1),r_pb) ,xmin=x_min,xmax=x_max_new/x_max_new,color='g',linestyle='--')



plt.xlim(x_min, x_max_new)

if m_extra == m_new - m_old:
	plt.legend( [cmin_handle,old_handle,new_handle,ext_handle,last_handle] , ['      ','      ','      ','      ','      '] , loc='upper right', bbox_to_anchor=(1.4, 1.025) )
else:
	plt.legend( [cmin_handle,old_handle,new_handle,ext_handle]             , ['      ','      ','      ','      ']           , loc='upper right', bbox_to_anchor=(1.4, 1.025) )

plt.xticks([])
# plt.yticks([])

# plt.yticks( log([20,200,1000],r_pb).round(1)  , ['      ', '      ', '      '] )
plt.xticks(   [x_min, x_max_old, x_max_new]   , ['0.0', '1.0', r'1.0+∆'] )


plt.xlabel( 'Selectivity'     )
plt.ylabel( 'Cost (log-scale)')




plt.savefig('1d.PNG',format='PNG',dpi=500,bbox_inches='tight')
plt.show()