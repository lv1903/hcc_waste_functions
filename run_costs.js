var w = require("./hcc_waste_functions.js");

var credentials = "B1xdinQcW:durley15";
var output_datasetId = "rJM6-SVjf";

var cost_datasetId = "HklQUiZ5f";	
var tonnage_datasetId = "B1bg6oLif";


w.cost_by_HWRC_Waste_Contract_Move(credentials, output_datasetId, cost_datasetId, tonnage_datasetId, function(res){
	console.log(res)
});