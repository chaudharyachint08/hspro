## CHECKPOINT, Do this later in your thesis, not for PPT

import numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sns

from sklearn.datasets import make_regression

X, Y, lin_coef, AVG, SEA, AEA, X_min_step, AVGp, SEAp, AEAp = None, None, None, None, None, None, None, None, None, None

def ExpAvg(alpha=0.5):
	global X, Y, lin_coef, AVG, SEA, AEA, X_min_step, AVGp, SEAp, AEAp
	X, Y = np.array(X), np.array(Y)
	AVG = np.cumsum(Y)/np.linspace(1,Y.shape[0],Y.shape[0])
	SEA = np.copy(Y)
	for ix in range(1,Y.shape[0]):
		step = 1 #
		SEA[ix] = (1-(1-alpha)**step)*(Y[ix]) + (0+(1-alpha)**step)*(SEA[ix-1])
	AEA = np.copy(Y)
	for ix in range(1,Y.shape[0]):
		step = (X[ix]-X[ix-1]) / X_min_step
		AEA[ix] = (1-(1-alpha)**step)*(Y[ix]) + (0+(1-alpha)**step)*(SEA[ix-1])
	return AVG, SEA, AEA

def ExpPredict(alpha=0.5):
	global X, Y, lin_coef, AVG, SEA, AEA, X_min_step, AVGp, SEAp, AEAp
	X, Y = np.array(X), np.array(Y)
	AVG = np.cumsum(Y)/np.linspace(1,Y.shape[0],Y.shape[0])
	SEA = np.copy(Y)
	for ix in range(1,Y.shape[0]):
		step = 1 #
		SEA[ix] = (1-(1-alpha)**step)*(Y[ix]) + (0+(1-alpha)**step)*(SEA[ix-1])
	AEA = np.copy(Y)
	for ix in range(1,Y.shape[0]):
		step = (X[ix]-X[ix-1]) / X_min_step
		AEA[ix] = (1-(1-alpha)**step)*(Y[ix]) + (0+(1-alpha)**step)*(SEA[ix-1])
	return AVG, SEA, AEA


def MAPE(Y_real,Y_pred):
	return np.mean(np.abs((Y_real-Y_pred)/Y_real))

# X = X/min_step

def f(alpha=0.5, predict=True):
	global X, Y, lin_coef, AVG, SEA, AEA, X_min_step, AVGp, SEAp, AEAp
	X, Y, lin_coef = make_regression(n_samples=100, n_features=1, n_informative=1, n_targets=1,\
		bias=0, noise=2, shuffle=False, coef=True, random_state=42)
	X = X[:,0]
	X, Y = np.sort(np.array([X,Y]))
	X_min_step = (X[1:]-X[:-1]).min()
	if predict:
		AVG, SEA, AEA = ExpPredict(X,Y,alpha)
	else:
		AVG, SEA, AEA = ExpAvg(X,Y,alpha)
	return (MAPE(Y,AVG), MAPE(Y,SEA), MAPE(Y,AEA))

def plot(alpha=0.5, ln=None, start=True, predict=True):
	global X, Y, lin_coef, AVG, SEA, AEA, X_min_step, AVGp, SEAp, AEAp
	_ = f(alpha, predict)
	X   = (  X[:ln] if start else (  X[-(ln+1):] if ln is not None else   X))
	Y   = (  Y[:ln] if start else (  Y[-(ln+1):] if ln is not None else   Y))
	plt.plot(X,Y  ,label='True Output')
	if predict:
		AVGp = (AVGp[:ln] if start else (AVGp[-(ln+1):] if ln is not None else AVGp))
		SEAp = (SEAp[:ln] if start else (SEAp[-(ln+1):] if ln is not None else SEAp))
		AEAp = (AEAp[:ln] if start else (AEAp[-(ln+1):] if ln is not None else AEAp))
		plt.plot(X,AVGp,label='Running Average')
		plt.plot(X,SEAp,label='Simple Exponential Average')
		plt.plot(X,AEAp,label='Adaptive Exponential Average')
	else:
		AVG  = (AVG[:ln]  if start else (AVG[-(ln+1):]  if ln is not None else  AVG))
		SEA  = (SEA[:ln]  if start else (SEA[-(ln+1):]  if ln is not None else  SEA))
		AEA  = (AEA[:ln]  if start else (AEA[-(ln+1):]  if ln is not None else  AEA))
		plt.plot(X,AVG ,label='Running Average')
		plt.plot(X,SEA ,label='Simple Exponential Average')
		plt.plot(X,AEA ,label='Adaptive Exponential Average')
	plt.legend()
	plt.grid(True)
	plt.show()
	return _