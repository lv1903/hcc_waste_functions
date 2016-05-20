var w = require("./hcc_waste_functions.js");

var credentials = "B1xdinQcW:durley15";

var output_datasetId = "B1bg6oLif";

var ratio_datasetId = "rJZ0lHXoM";	
var behaviour_datasetId = "SygHkQnuz";

var NID = "11111111111111111111000000";

w.tonnage_by_HWRC_Waste_Contract_Move(NID, credentials, output_datasetId, ratio_datasetId, behaviour_datasetId, function(res){
	console.log(res)
});

