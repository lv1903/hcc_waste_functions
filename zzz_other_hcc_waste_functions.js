

		
// var ratio_databaseID = "rJeM71xAZ";	
// var behaviour_databaseId = "SygHkQnuz";

// var ratio_path = '/v1/datasets/' + ratio_databaseID + '/data';
// var behaviour_path = '/v1/datasets/' + behaviour_databaseId + '/data';

// var ratio_options = {
	// host: 'q.nqminds.com',
	// path: ratio_path
// };		
// var behaviour_options = {
	// host: 'q.nqminds.com',
	// path: behaviour_path
// };		


// w.nqm_tbx_query(ratio_options, function(aRatio){	
	// w.nqm_tbx_query(behaviour_options, function(aBehaviour){

		// w.tonnage_by_HWRC_Waste_Contract_Move_2("11111111111111111111111111", aRatio, aBehaviour, function(res){
			// console.log(res)
		// });
	
	// });	
// });	



	// }
	
/* 	match_NID: function(aNID, aSubNID){		
		for(var index in aNID){
			if(aSubNID[index] != "-"){
				if(aSubNID[index] != aNID[index]){
					return false
				}
			}
		}
		return true
	}, */
	
	
	/* tonnage_by_HWRC_Waste_Contract_Move_2: function(nid, aRatio, aBehaviour, callback){
		
		//not tested
		
		var oOutput = {};
		
		aNID = nid.split("");
		// console.log(aNID)
		
		for(var behaviour_index in aBehaviour){
			
			var behaviour_record = aBehaviour[behaviour_index]
			var aSubNID = behaviour_record.subNID.split("") ;
			
			// console.log(behaviour_record)
			
			//check the behaviour record subNID matches the NID
			if(this.match_NID(aNID, aSubNID)){
				
				var sid = behaviour_record.SID;
				var hwrc = behaviour_record.HWRC;
				var waste = behaviour_record.Waste_Type;
				var net_demand = behaviour_record.net_demand;
				
				//loop through ratios and add records that match
				for(var ratio_index in aRatio){
					ratio_record = aRatio[ratio_index];
					if(ratio_record.HWRC==hwrc){
						if(ratio_record.Waste_Type==waste){
							
							var contract = ratio_record.Contract;
							var move = ratio_record.First_Movement;
							
							var output_id = hwrc + "|" + waste + "|" + nid + "|" + sid + "|" + contract + "|" + move ;
							
							if(!oOutput.hasOwnProperty(output_id)){
								oOutput[output_id] = {};
								oOutput[output_id].HWRC = hwrc;
								oOutput[output_id].Waste_Type = waste;
								oOutput[output_id].NID = nid;
								oOutput[output_id].SID = sid;
								oOutput[output_id].Contract = contract;
								oOutput[output_id].First_Movement = move;
								oOutput[output_id].Tonnage = 0;
							}
							
							oOutput[output_id].Tonnage += ratio_record.ratio * net_demand
							
						}
					}
					
					
				}
				
				
			}
			// break
		}
		
		for(output_id in oOutput){
			console.log(output_id)
		}
		
		callback("done")

	}

	
	 */	