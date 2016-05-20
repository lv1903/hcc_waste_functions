var w = require("./hcc_waste_functions.js");

var credentials = "B1xdinQcW:durley15";


var	nid_datasetId = "S1ZiucIjM";
var	tonnage_datasetId = "B1geUPvoz";
var	ratio_datasetId = "rJZ0lHXoM";
var	behaviour_datasetId = "SygHkQnuz";

w.run_NID_permutations(credentials, nid_datasetId, tonnage_datasetId, ratio_datasetId, behaviour_datasetId, function(res){
	console.log(res)
});