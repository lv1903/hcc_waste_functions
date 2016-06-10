var w = require("./hcc_waste_functions.js");

var credentials = "B1xdinQcW:durley15";

var datasetID = {};

datasetID.nid_datasetId = "rkx-QLiEQ";
datasetID.ratio_datasetId = "ByxjT0qNX";
datasetID.behaviour_datasetId = "SylLs44d4";
datasetID.displacements_datasetId = "B1eXxfjEX";

datasetID.tonnage_datasetId = "rJeTC5HON";
datasetID.output_datasetId = "SkgFcSoEX";

var NID = "00111000111111010010111111";
// var NID = "11111111111111111111111111";

//used to cache query data
var qCache = {};		
qCache.aRatio = [];
qCache.aBase = [];

console.log(NID + ": " + NID.length);

if(NID.length == 26){
	w.tonnage_by_HWRC_Waste_Contract_Move(NID, credentials, datasetID, qCache, function(res){
		console.log(res)
	});
}

