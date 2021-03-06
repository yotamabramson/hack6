<script src="http://d3js.org/topojson.v0.min.js"></script>
<script>
var width = 960,
    height = 500;

var projection = d3.geo.mercator()
    .center([0, 5 ])
    .scale(200)
    .rotate([-180,0]);

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height);

var path = d3.geo.path()
    .projection(projection);

var g = svg.append("g");
    
var max=21, data = [];

var colorScale=d3.scale.linear().domain([0,100]).range(["white","blue"]);    
var sizeScale = d3.scale.linear().domain([0,100]).range([5,100]);    

var dataIndex = 0;
setInterval(function() {
    dataIndex = dataIndex % 2;
    updateData(dataIndex);
    dataIndex += 1;
}, 5000);
var ANIMATION_DURATION = 1000;
function updateData(i) {
    
// load and display the World
d3.json("world-110m2.json", function(error, topology) {

// load and display the cities
d3.csv("cities" + i + ".csv", function(error, data) {
//    console.log("cities" + i + ".csv", "duration ", ANIMATION_DURATION);
    
    var elem = g.selectAll("circle").data(data, function(d) { return d.city;})

    var circle = elem.enter().append("circle");	  
       
      circle.attr("cx", function(d) {
               return projection([d.lon, d.lat])[0];
       })
       .attr("cy", function(d) {
               return projection([d.lon, d.lat])[1];
       })
       .style("fill", function(d){ 
            return colorScale(d.value); 
        })
       .attr("r", function(d){
            return sizeScale(d.value2);
        });
    
    elem.transition().duration(500).attr('r', function(d){
            return sizeScale(d.value2);
        });
    
    circle.append("svg:title")
        .text(function(d) { return d.country; });
//       

//        elem.exit()
//            .select('circle')
//            .transition()
//            .duration(ANIMATION_DURATION)
//            .attr('r', 0);
//        elem.exit()
//            .select('circle')
//            .transition()
//            .duration(ANIMATION_DURATION)
//            .remove();
});
        

g.selectAll("path")
      .data(topojson.object(topology, topology.objects.countries)
          .geometries)
    .enter()
      .append("path")
      .attr("d", path)
});

// zoom and pan
var zoom = d3.behavior.zoom()
    .on("zoom",function() {
        g.attr("transform","translate("+ 
            d3.event.translate.join(",")+")scale("+d3.event.scale+")");
        g.selectAll("circle")
            .attr("d", path.projection(projection));
        g.selectAll("path")  
            .attr("d", path.projection(projection)); 

  });

svg.call(zoom)
}

</script>
