import sys, os, argparse, matplotlib, numpy as np, re, math, csv, calendar

matplotlib.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator, LinearLocator

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_results_file
	
def create_nice_labels(labels):
	nice_labels = []
	for label in labels:
		year, month = map(int, re.match("(\d+)\-(\d+)", label).groups())
		nice_labels.append(calendar.month_abbr[month] + " " + str(year)[2:])
	return nice_labels
	
def line_plot(result_file, dest, order=None, inset_result_file=None, inset_title=None, title=None, legend_pos=None, legend_cols=None, xlabel=None, ylabel=None):
	headers, labels, ys = read_results_file(result_file)
	cats = headers[1:]
	x = sorted([int(num_clicks) for num_clicks in labels])
					
	# reorder the data that will be plotted and set legend labels
	legend_labels = []
	if order is not None:
		new_ys = []
		for catid, catname in order:
			new_ys.append(ys[cats.index(catid)])
			legend_labels.append(catname)
		ys = new_ys
	else:
		legend_labels = cats[:]

	fig = plt.figure()
	ax = fig.add_subplot(111)

	colors = ['#fdb863', '#e66101', '#b2abd2', '#5e3c99']
	markers = ['s', 'o', '^', 'x']

	curves = []
	for i in range(len(ys)):
		curve = ax.plot(x, ys[i], linestyle="-", color=colors[i], marker=markers[i], markerfacecolor='none', markeredgecolor=colors[i])[0]
		#curve = ax.errorbar(x, ys[i], errors[i], color=colors[i])
		curves.append(curve)
	
	# display a subfigure id if this is going to be part of a subfigure
	plt.figtext(0.025, 0.025, "b", fontsize=30, weight='bold')

	legend_str = legend_pos if legend_pos is not None else "upper right"
	cols = legend_cols if legend_cols is not None else 2
	ax.legend(curves, legend_labels, loc=legend_str, shadow=False, ncol=cols, prop=FontProperties(size=18), numpoints=1)

	plt.xlim(x[0] - .2 * x[0], x[-1] + .9 * x[-1])
	#plt.ylim(ys[0][0] - .2 * ys[0][0], ys[0][-1] + .05 * ys[0][-1])

	for tick in ax.xaxis.get_major_ticks():
		tick.label.set_fontsize(22)
		
	for tick in ax.yaxis.get_major_ticks():
		tick.label.set_fontsize(22)
			
	if xlabel is not None:
		ax.set_xlabel(xlabel, fontsize=22)
			
	if ylabel is not None:
		ax.set_ylabel(ylabel, fontsize=22)
	
	if title is not None:
		plt.title(title, fontsize=22)

	ax.set_xscale('log')
	plt.grid(True, color="#bbbbbb")
		
	# add an inset to the figure
	if inset_result_file is not None:
		inset_ax = plt.axes([.6, .2, .35, .26])
		
		inset_headers, inset_labels, inset_ys = read_results_file(inset_result_file)
		inset_x = sorted([int(num_clicks) for num_clicks in inset_labels])

		# reorder the data that will be plotted and set legend labels
		if order is not None:
			new_inset_ys = []
			for catid, catname in order:
				new_inset_ys.append(inset_ys[cats.index(catid)])
			inset_ys = new_inset_ys

		inset_curves = []
		for i in range(len(inset_ys)):
			curve = inset_ax.plot(inset_x, inset_ys[i], linestyle="-", color=colors[i], marker=markers[i], markerfacecolor='none', markeredgecolor=colors[i])[0]
			inset_curves.append(curve)
		inset_ax.set_xscale('log')

		if inset_title is not None:
			inset_ax.set_title(inset_title)

	pp = PdfPages(dest)	
	fig.savefig(pp, format='pdf')
	pp.close()
	plt.close()
	
if __name__ == "__main__":
	line_plot(
		os.path.join(os.getenv("TR"), "evstv-level2-month.txt"),
		os.path.join(os.getenv("TP"), "entropy-vs-traffic-volume.pdf"), 
		ylabel="Entropy", xlabel="Clicks", legend_pos="upper left", legend_cols=1,
		order=[('email', 'Mail'), ('social', 'Social Media'), ('search', 'Search')],
		inset_result_file=os.path.join(os.getenv("TR"), "news-evstv-full-month.txt"), inset_title="News"
    )
