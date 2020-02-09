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


fig = plt.figure()
ax = fig.gca(projection='3d')

# Make data.
max_scale, max_dim, plot_res = 5, 2, 10

max_opt_call = 10**1

X_copy = X = np.linspace(2, max_scale, plot_res)
Y = np.linspace(1, max_dim,   plot_res)

X, Y = np.meshgrid(X, Y)

Z = np.log2(X)**Y

# Plot the surface.
# surf = ax.plot_surface(X, Y, Z, cmap=cm.coolwarm, linewidth=0, antialiased=False)

# surf = ax.plot_surface(X, Y, Z, cmap='viridis', linewidth=0, antialiased=False)
surf = ax.scatter(X, Y, Z, cmap='viridis', linewidth=0, antialiased=False)

# Customize the z axis.
# ax.set_zlim(0, None)

ax.zaxis.set_major_locator(LinearLocator(10))
ax.zaxis.set_major_formatter(FormatStrFormatter('%.02f'))

# Add a color bar which maps values to colors.
# fig.colorbar(surf, shrink=0.5, aspect=5)

plt.show()
