var w = require("./hcc_waste_functions.js");

var credentials = "B1xdinQcW:durley15";
var nid_datasetId = "S1ZiucIjM";

var subNID = "-----111111111111111111111";

w.generate_NID_permutations(subNID, credentials, nid_datasetId, function(res){
	console.log(res)
});