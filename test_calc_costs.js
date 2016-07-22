 var calculateCostOutput = function(aCost, aTonnage){

    var data = [];
    var fail = [];

    for(var tonnage_index in aTonnage){
      var t = aTonnage[tonnage_index];

      //find the matching cost record and apply cost per tonne to tonnage
      for(var cost_index in aCost){
        var c = aCost[cost_index];

        if(c.First_Movement == t.First_Movement){
          if(c.HWRC == t.HWRC){
            if(c.Waste_Type == t.Waste_Type){
              if(c.Contract == t.Contract){

                var oRecord = {};

                var Cost;
                if(t.Tonnage > 0){
                  Cost = t.Tonnage * c.Cost;
                  Cost = Math.round(100 * Cost) / 100;
                } else {
                  Cost = 0;
                }

                var Delta_Cost;
                if(t.Delta_Tonnage > 0){
                  Delta_Cost = t.Delta_Tonnage * c.Cost;
                  Delta_Cost = Math.round(100 * Delta_Cost) / 100;
                } else {
                  Delta_Cost = 0;
                }



                oRecord.NID = t.NID
                oRecord.SID = t.SID
                oRecord.HWRC = t.HWRC;
                oRecord.Waste_Type = t.Waste_Type;
                oRecord.Contract = t.Contract;
                oRecord.First_Movement = t.First_Movement;
                oRecord.Tonnage = t.Tonnage;
                oRecord.Cost = Cost;
                oRecord.Delta_Tonnage = t.Delta_Tonnage;
                oRecord.Delta_Cost = Delta_Cost;

                oRecord.Winter = t.Winter;
                oRecord.Monday = t.Monday;
                oRecord.Tuesday = t.Tuesday;
                oRecord.Wednesday = t.Wednesday;
                oRecord.Thursday = t.Thursday;
                oRecord.Friday = t.Friday;
                oRecord.Saturday = t.Saturday;
                oRecord.Sunday = t.Sunday;


                data.push(oRecord);
                break
              }
            }
          }
        }
      }
    }
    return data
  };
  

var config = {
  "rootURL": "http://localhost:7777",
  "authServerURL": "https://tdx.nqminds.com",
  "RCLookupResourceId": "S1lWjFHVV",
  "baseQueryURL": "https://q.nqminds.com",
  "baseCommandURL": "https://cmd.nqminds.com",
  "outputDatasetSchema": "dataset",
  "costDatasetSchema": "HccWasteCost",
  "tonnageDataset":"H1eCZDVWr"
}

var datasetID = {};

datasetID.nid_datasetId = "rkx-QLiEQ";
datasetID.ratio_datasetId = "ByxjT0qNX";
datasetID.behaviour_datasetId = "SylLs44d4";
datasetID.displacements_datasetId = "B1eXxfjEX";
datasetID.cost_datasetId = "HkehyysVm";

datasetID.tonnage_datasetId = "H1eCZDVWr";
datasetID.output_datasetId = "HygxXEFSB";
  
  
var tdxAPI = (new (require("nqm-api-tdx"))(config));
  
  
var aCost;
var aTonnage;

var nextTonnage = function(max, index, delta, cb){
	
		
	
	if(index < max){
		
		console.log(Math.round(100*index/max) + "%: " + index + " out of " + max)
		
		tdxAPI.query("datasets/" + datasetID.tonnage_datasetId +  "/data", null, null, {"skip":index, "limit":delta}, function(err, qres, res) {
			
		  if(err){
			  // return cb("Tonnage Query Failed: " + err)
		  }	else {			  
			  aTonnage = res.data	
			  var outputData = calculateCostOutput(aCost, aTonnage);
			  
			  console.log("add " + outputData.length)
			  
			  tdxAPI.addDatasetData(datasetID.output_datasetId, outputData, function(err, res){
				  
				  if(err){
					  console.log(err)
					  // return cb("Add tonnage data failed: " + err)
				  }
				  
				  nextTonnage(max, index + delta, delta, cb)
				  
			  });			  
		  }			
	  })		
		
	} else {
		cb("data ready")
	}
	
	
}


			
tdxAPI.truncateDataset(datasetID.output_datasetId, function(err) {
	if (err) {
	  // return cb(err);
	}
	
	tdxAPI.query("datasets/" + datasetID.cost_datasetId +  "/data", null, null, {"limit":10000}, function(err, qres, res) {
		if(err){
			return cb("Cost Query Failed: " + err)
		}
		
		aCost = res.data
		
		console.log(aCost.length)
		
		tdxAPI.query("datasets/" + datasetID.tonnage_datasetId +  "/count", null, null, null, function(err, qres, res) {
			
		  if(err){
			// return cb("Tonnage Count Query Failed: " + err)
		  }
			
		  var max = res.count;
		  max = 55
		  
		  nextTonnage(max, 0, 10, function(res){
			console.log(res)
		  }) 	
		


		});
	});
		
	
});