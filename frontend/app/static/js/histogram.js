// set the dimensions and margins of the graph
var margin = {top: 20, right: 20, bottom: 30, left: 40},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

// set the ranges
var x = d3.scaleLinear()
          .range([0, width]);
var y = d3.scaleLog()
          .range([height, 0]);

// append the svg object to the body of the page
// append a 'group' element to 'svg'
// moves the 'group' element to the top left margin
var svg = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");

// get the data
d3.csv("/static/csv/clst100k.csv", function(error, data) {
  if (error) throw error;

  // format the data
  data.forEach(function(d) {
    d.clst_size = +d.clst_size;
    d.clst_num = +d.clst_num;
  });

  // Scale the range of the data in the domains
  x.domain([1, d3.max(data, function(d) { return d.clst_size; })]);
  y.domain([1, d3.max(data, function(d) { return d.clst_num; })]);

  // bar width
  var barWidth = width / d3.max(data, function(d) { return d.clst_size; });

  // append the rectangles for the bar chart
  svg.selectAll(".bar")
      .data(data)
    .enter().append("rect")
      .attr("class", "bar")
      .attr("x", function(d) { return x(d.clst_size); })
      .attr("width", barWidth)
      .attr("y", function(d) { return y(d.clst_num); })
      .attr("height", function(d) { return height - y(d.clst_num); });

  // add the x Axis
  svg.append("g")
      .attr("transform", "translate(0," + height + ")")
      .call(d3.axisBottom(x));

  // add the y Axis
  svg.append("g")
      .call(d3.axisLeft(y));

});

