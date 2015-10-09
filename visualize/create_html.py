import sys, os, glob, numpy, re, math
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

HTML = """<!DOCTYPE html>
<meta charset="utf-8">
<style>

body {
  font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
  margin: auto;
  position: relative;
  width: 960px;
}

form {
  position: absolute;
  right: 10px;
  top: 10px;
}

.node {
  border: solid 1px white;
  font: 10px sans-serif;
  line-height: 12px;
  overflow: hidden;
  position: absolute;
  text-indent: 2px;
}

</style>

<body>

<button id="save_as_svg">Save as SVG</button>
<button id="save_as_pdf">Save as PDF</button>
<button id="save_as_png">Save as High-Res PNG</button>

<form id="svgform" method="post" action="/cgi-bin/convert_svg.pl">
 <input type="hidden" id="output_format" name="output_format" value="">
 <input type="hidden" id="data" name="data" value="">
</form>

<div id="treemap"></div>

<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min.js"></script>
<script src="http://d3js.org/d3.v3.min.js"></script>
<script>

var margin = {top: 40, right: 10, bottom: 10, left: 10},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

var color = d3.scale.category20c();

var treemap = d3.layout.treemap()
  .size([width, height])
  .sticky(true)
  .value(function(d) { return d.size; });

var svg = d3.select("#treemap").append("svg")
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)

d3.json("%s", function(error, root) {
  var node = svg.datum(root).selectAll(".node")
    .data(treemap.nodes)
    .enter().append("g")
    .attr("class", "node")
    .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

  node.append("rect")
    .attr("width", function(d) { return d.dx; })
    .attr("height", function(d) {return d.dy})
    .style("fill", function(d) { return %s; })
    .style("stroke", "#ffffff")
    .style("stroke-width", "1.5");

  node.append("text")
    .attr("dx", "0.2em")
    .attr("dy", "1em")
    .style("font-size", "20pt")
    .text(function(d) { return d.name.substring(0, d.dx / 14); });
});

function submit_download_form(output_format) {
    // Get the d3js SVG element
    var svg = document.getElementById("treemap");
    // Extract the data as SVG text string
    var svg_xml = (new XMLSerializer).serializeToString(svg);

    // Submit the form to the server.
    // The result will be an attachment file to download.
    var form = document.getElementById("svgform");
    form['output_format'].value = output_format;
    form['data'].value = svg_xml ;
    form.submit();
}

$(document).ready(function() {
	$("#save_as_svg").click(function() { submit_download_form("svg"); });

	$("#save_as_pdf").click(function() { submit_download_form("pdf"); });

	$("#save_as_png").click(function() { submit_download_form("png"); });
});

</script>
</body>
"""

def compute_entropy(probs):
	h = 0
	for p in probs:
		h += -p*numpy.log2(p)
	return h

def compute_entropy_on_json(filepath):
	clicks = []
	with open(filepath, 'r') as f:
		for line in f:
			if "name" in line and "size" in line:
				m = re.search('"size": (\\d+)', line)
				if m is not None:
					count = int(m.group(1))
					clicks.append(count)
	
	total = numpy.sum(clicks)
	probs = [float(count) / total for count in clicks]
	return compute_entropy(probs)

def create_html(src, dest, json_href, color):
	print "Processing %s" % src
	with open(dest, 'w') as destf:
		destf.write(HTML % (json_href, color))

def worker(params):
	return create_html(*params)

def run_in_parallel(files, dest_dir, href_dir, num_processes):
	all_html = """<!DOCTYPE html>
<meta charset="utf-8"><body>
"""
	params = []
	for dev, f in files:
		remainder, filename = os.path.split(f)
		_, category = os.path.split(remainder)

		if category == 'search':
			color = 'd3.rgb(178,171,210)'
		elif category == 'social':
			color = 'd3.rgb(230,97,1)'
		elif category == 'email':
			color = 'd3.rgb(253,184,99)'
		else:
			color = 'd3.rgb(254,203,0)'
		destf = os.path.join(dest_dir, category, os.path.splitext(filename)[0] + ".html")
		all_html += "<a href='%(cat)s/%(file)s'>%(cat)s/%(file)s</a>(%(dev)f)<br />\n" % {
			'cat': category,
			'file': os.path.splitext(filename)[0] + ".html",
			'dev': dev
		}
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((f, destf, os.path.join(href_dir, category, filename), color))

	p = Pool(processes=num_processes)
	results = p.map(worker, params)
	
	all_html += "</body></html>"
	with open(os.path.join(dest_dir, "index.html"), 'w') as indexf:
		indexf.write(all_html)

def get_sorted_json_files(files):
	h = {}
	for f in files:
		remainder, filename = os.path.split(f)
		_, category = os.path.split(remainder)
		if category not in h:
			h[category] = {}
		h[category][f] = compute_entropy_on_json(f)

	avgh = {}
	for cat in h:
		avgh[cat] = numpy.average(h[cat].values())

	index = []
	for cat in h:
		for f in h[cat]:
			index.append((numpy.abs(avgh[cat]-h[cat][f]), f))
			
	print avgh
	return sorted(index)
def main():
	run_in_parallel(
		get_sorted_json_files(glob.glob(os.path.join(os.getenv("TR"), "json-week-level2", "*", "*"))),
		os.path.join(os.getenv("TR"), "html"),
		"/json",
		16
	)

if __name__ == "__main__":
	main()
