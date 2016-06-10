var w = require("./hcc_waste_functions.js");

var credentials = "B1xdinQcW:durley15";

var datasetID = {};
datasetID.nid_datasetId = "rkx-QLiEQ";

var subNID = "--111---111111------111-11";

console.log(subNID + ": " + subNID.length);
if(subNID.length == 26){
	w.generate_NID_permutations(subNID, credentials, datasetID, function(res){
		console.log(res)
	});
}