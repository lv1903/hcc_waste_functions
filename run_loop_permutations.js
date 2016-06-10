var w = require("./hcc_waste_functions.js");

var credentials = "B1xdinQcW:durley15";

var datasetID = {};

datasetID.nid_datasetId = "rkx-QLiEQ";
datasetID.ratio_datasetId = "ByxjT0qNX";
datasetID.behaviour_datasetId = "SylLs44d4";
datasetID.displacements_datasetId = "B1eXxfjEX";
datasetID.cost_datasetId = "HkehyysVm";

datasetID.tonnage_datasetId = "rJeTC5HON";
datasetID.output_datasetId = "SkgFcSoEX";


w.run_NID_permutations(credentials, datasetID, function(res){
	console.log(res)
});