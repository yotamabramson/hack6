ANIMATION_DURATION = 3000

var svg = d3.select("#titles");
var width = svg.style("width");
var height = svg.style("height");
width = width.substr(0, width.length - 2)
height = height.substr(0, height.length - 2)

var format = d3.format(",d");

//var color = d3.scaleOrdinal(d3.schemeCategory20c);

var pack = d3.pack()
    .size([width, height])
    .padding(1.5);

var dataN = 23;
var dataIndex = 1;
setInterval(function() {
    dataIndex += 1;
    if (dataIndex > dataN) {
        dataIndex = 1;
    }
    updateData(dataIndex);
}, ANIMATION_DURATION);

function updateData(dataIndex) {
    d3.csv("data/words_" + dataIndex + ".csv", function (d) {
        d.value = +d.value;
        if (d.value) return d;
    }, function (error, classes) {
        if (error) throw error;

        var root = d3.hierarchy({children: classes})
            .sum(function (d) {
                return d.value;
            })
            .each(function (d) {
                if (id = d.data.id) {
                    var id, i = id.lastIndexOf(".");
                    d.id = id;
                    d.package = id.slice(0, i);
                    d.class = id.slice(i + 1);
                }
            });

        var elements = svg.selectAll(".node")
            .data(pack(root).leaves(), function(d) { return d.data.id;});
        node = elements.enter().append("g")
            .attr("class", "node")
            .attr("transform", function (d) {
                return "translate(" + d.x + "," + d.y + ")";
            });

        node.append("circle")
            .attr("id", function (d) {
                return d.id;
            })
            .style("fill", function (d) {
                return color(d.package);
            })
            .transition()
            .duration(ANIMATION_DURATION)
            .attr("r", function (d) {
                return d.r;
            });

        elements.select("circle")
            .transition()
            .duration(ANIMATION_DURATION)
            .attr("r", function (d) {
                return d.r;
            });

        //  node.append("clipPath")
        //      .attr("id", function(d) { return "clip-" + d.id; })
        //    .append("use")
        //      .attr("xlink:href", function(d) { return "#" + d.id; });

        node.append("text")
        //      .attr("clip-path", function(d) { return "url(#clip-" + d.id + ")"; })
            .selectAll("tspan")
            .data(function (d) {
                return d.class.split(/(?=[A-Z][^A-Z])/g);
            })
            .enter().append("tspan")
            .attr("x", function (d, i, nodes) {
                return (i - 20);
            })
            .attr("y", function (d, i, nodes) {
                return 13 + (i - nodes.length / 2 - 0.5) * 10;
            })
			.style("fill", function (d) {
                return 000;
            })
			.style("font-weight", function (d) {
                return 800;
            })
            .text(function (d) {
                return d;
            })
            .attr("opacity", 0)
            .transition()
            .delay(ANIMATION_DURATION / 2)
            .attr("opacity", 1);

        node.append("title")
            .text(function (d) {
                return d.id + "\n" + format(d.value);
            });

        elements.exit()
            .select('text')
            .attr('opacity', 1)
            .transition()
            .duration(ANIMATION_DURATION)
            .attr('opacity', 0);
        elements.exit()
            .select('circle')
            .transition()
            .duration(ANIMATION_DURATION)
            .attr('r', 0);
        elements.exit()
            .transition()
            .duration(ANIMATION_DURATION)
            .remove();
    });
}

updateData(dataIndex);