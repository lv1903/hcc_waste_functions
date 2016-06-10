
module.exports = {	
	
	nqm_tbx_query: function(options, callback){
	//requests data from tbx and returns body as array of records

		var http = require("http");

		var req = http.get(options, function(res) {
			var bodyChunks = [];
			res.on('data', function(chunk) {
				bodyChunks.push(chunk);
			}).on('end', function() {
				var body = Buffer.concat(bodyChunks);
				callback(JSON.parse(body).data)
			})
		});
		req.on('error', function(e) {
			console.log('ERROR: ' + e.message);
		});
	},
	

	create_behaviour_path: function(NID, behaviour_datasetId){
		
		//group string
		var group = '{"$group":{"_id":{"HWRC":"$HWRC","Waste_Type":"$Waste_Type","SID":"$SID"},"net_demand":{"$sum":"$net_demand"}}}';	
		
		//match string
			var aNID = NID.split("");
		var max_i = aNID.length - 1;
		var match = '{"$match":{"$and":[' //+ '{"HWRC":"Aldershot"},{"Waste_Type":"AMENITY"},{"SID":"2015"},'
		for(var i in aNID){		
			var hwrcId = "HWRC" + ("0" + i).substr(-2);
			var bit = aNID[i];
			match += '{"$or":[{"' + hwrcId + '":"' + bit + '"},{"' + hwrcId + '":"-"}]}';
			if(i < max_i){match += ','}
		}
		match += ']}}';	
			
		var path = '/v1/datasets/' + behaviour_datasetId + '/aggregate?opts={"limit":1000000}&pipeline=[' + match + ',' + group + ']';	
		return path	
	},
	
	get_tonnage_by_HWRC_Waste_Contract_Move_data: function(NID, aRatio, aBehaviour){
		
		var data = [];
		var aSID = ["2015", "2021"];
		
		// var oTest = {};
						
		for(var sid_index in aSID){					
			var sid = aSID[sid_index];					
			
			for(var ratio_index in aRatio){		
			
				var ratio = aRatio[ratio_index];						
					var hwrc = ratio["HWRC"];
					var waste = ratio["Waste_Type"];	

					var net_demand = 0;	//set it for zero incase there is no matching behaviour record
					
					//extract the tonnage that matches the ratio record
					for(behaviour_index in aBehaviour){							
						
						var o = aBehaviour[behaviour_index]._id;							
						
						if(o.HWRC==hwrc){
							if(o.Waste_Type==waste){
								if(o.SID==sid){
									net_demand = aBehaviour[behaviour_index].net_demand;
									break
								}									
							}
						}
					}	

					var oRecord = {};
					var Tonnage;
					if(net_demand > 0){
						Tonnage = Math.round(100 * ratio["ratio"] * net_demand) / 100;
						
					} else {
						Tonnage = 0;
					}
					
					oRecord["NID"] = NID
					oRecord["SID"] = sid
					oRecord["HWRC"] = hwrc;
					oRecord["Waste_Type"] = waste;
					oRecord["Contract"] = ratio["Contract"];
					oRecord["First_Movement"] = ratio["First_Movement"];
					oRecord["Tonnage"] = Tonnage;						
					data.push(oRecord);	

					// if(sid=="2021"){
						// if(waste=="GREEN AM"){
							// if(NID=="00111000111111010010111111"){
								// if(ratio["Contract"]=="Contract"){
									// console.log(oRecord)
									
								// }
							// }
						// }
					// }

			
			}
		}
		// var test_tonnage = 0
		// for(var h in oTest){
			// test_tonnage += oTest[h]
		// }
		// console.log(test_tonnage)
		return data
		
	},
	
	calculate_delta: function(data, base_data){
		
		sum = 0
		sumBase = 0
		
		for(var data_index in data){
			var record = data[data_index]
			
			for(base_index in base_data){
				base_record = base_data[base_index];
				
				if(base_record.SID == "2015"){	//could strip out 2021 data				
					if(base_record.HWRC == record.HWRC){
						if(base_record.Waste_Type == record.Waste_Type){
							if(base_record.Contract == record.Contract){
								if(base_record.First_Movement == record.First_Movement){
									
									record.Delta_Tonnage = Math.round(100 * (record.Tonnage - base_record.Tonnage)) / 100;

												// if(record.SID == "2021")
													// if(base_record.Waste_Type == "AMENITY"){	//could strip out 2021 data				
														// if(base_record.HWRC == "Eastleigh"){
															

															// console.log(record)
															// console.log(base_record)
															// console.log(record.Tonnage + " : " + base_record.Tonnage)
															// console.log("*********")
															// sum += record.Tonnage
															// sumBase += base_record.Tonnage

														// }
													// }

									
								}								
							}							
						}
					}
				}
				
				
			}
		}
		// console.log("data: " + sum + "; base: " + sumBase)
		return data
		
	},
	
	calculate_displacements: function(data, aDisplacements){
		
		for(var data_index in data){
			
			var record = data[data_index]
			
			//for where there is no displacement rate
			record.Winter = 0;
			record.Monday = 0;
			record.Tuesday = 0;
			record.Wednesday = 0;
			record.Thursday = 0;
			record.Friday = 0;
			record.Saturday = 0;
			record.Sunday = 0;
			
			
			record.Winter		
		
			for(dis_index in aDisplacements){
				dis_record = aDisplacements[dis_index];
					
				if(dis_record.HWRC == record.HWRC){
					if(dis_record.Waste_Type == record.Waste_Type){
						record.Winter = Math.round(100 * record.Tonnage * dis_record.Winter) / 100;
						record.Monday = Math.round(100 * record.Tonnage * dis_record.Monday) / 100;
						record.Tuesday = Math.round(100 * record.Tonnage * dis_record.Tuesday) / 100;
						record.Wednesday = Math.round(100 * record.Tonnage * dis_record.Wednesday) / 100;
						record.Thursday = Math.round(100 * record.Tonnage * dis_record.Thursday) / 100;
						record.Friday = Math.round(100 * record.Tonnage * dis_record.Friday) / 100;
						record.Saturday = Math.round(100 * record.Tonnage * dis_record.Saturday) / 100;
						record.Sunday = Math.round(100 * record.Tonnage * dis_record.Sunday) / 100;
					}
				}

			}
		}
		return data
		
	},
	
	
	
	tonnage_by_HWRC_Waste_Contract_Move: function(NID, credentials, datasetID, qCache, callback){
		
		console.log(NID);
		
		var commands = require("./node_modules/nqm-json-import/lib/commands.js");
		
		var self = this;
	
		var ratio_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + datasetID.ratio_datasetId + '/data?opts={"limit":1000000}'
		};		
		var behaviour_options = {
			host: 'q.nqminds.com',
			path: self.create_behaviour_path(NID, datasetID.behaviour_datasetId)
		};		
		var displacement_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + datasetID.displacements_datasetId + '/data?opts={"limit":1000000}'
		};	
		
		
		base_NID = Array(27).join("1");
		
		// console.log(base_NID)
		// console.log(base_NID.length)
		
		var behaviour_base_options = {
			host: 'q.nqminds.com',
			path: self.create_behaviour_path(base_NID, datasetID.behaviour_datasetId)
		};	
		
		// console.log(behaviour_base_options.path)
		// console.log(behaviour_options.path)


		
		if(qCache.aRatio.length > 0 && qCache.aBase.length > 0 && qCache.aDisplacements.length > 0){ // use cache data if available
			
			self.nqm_tbx_query(behaviour_options, function(aBehaviour){				
				
				var data = self.get_tonnage_by_HWRC_Waste_Contract_Move_data(NID, qCache.aRatio, aBehaviour);
				
				data = self.calculate_delta(data, qCache.aBase);
				data = self.calculate_displacements(data, qCache.aDisplacements);
				
				var commandHost = "https://cmd.nqminds.com";
				commands.getAccessToken(commandHost, credentials, function(err, accessToken){	
					commands.upsertDatasetDataBulk(commandHost, accessToken, datasetID.tonnage_datasetId, data, function(res){	
						console.log(res)
						callback(NID, qCache)					
					});	
				});
				
			});
			
		} else { //else run queries and cache
			
			self.nqm_tbx_query(ratio_options, function(aRatio){		

				qCache.aRatio = aRatio;				 
			
				self.nqm_tbx_query(behaviour_base_options, function(aBase_Behaviour){				
					
					qCache.aBase = self.get_tonnage_by_HWRC_Waste_Contract_Move_data(base_NID, qCache.aRatio, aBase_Behaviour);
					
					// console.log("base:")
					// console.log(qCache.aBase[292])
					
					self.nqm_tbx_query(displacement_options, function(aDisplacements){
					
						qCache.aDisplacements = aDisplacements;
			
						self.nqm_tbx_query(behaviour_options, function(aBehaviour){				
							
							var data = self.get_tonnage_by_HWRC_Waste_Contract_Move_data(NID, qCache.aRatio, aBehaviour);
							
							// console.log("data:")
							// console.log(data[292])
							
							data = self.calculate_delta(data, qCache.aBase);	
							data = self.calculate_displacements(data, qCache.aDisplacements);
							
							var commandHost = "https://cmd.nqminds.com";					
							commands.getAccessToken(commandHost, credentials, function(err, accessToken){	
								commands.upsertDatasetDataBulk(commandHost, accessToken, datasetID.tonnage_datasetId, data, function(res){	
									console.log(res)
									callback(NID, qCache)					
								})
							});	
						});
						
					})	
				});	
			})
			
		}
	
	},
	
	

	get_cost_by_HWRC_Waste_Contract_Move_data: function(aCost, aTonnage){
		
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
	},
	
	cost_by_HWRC_Waste_Contract_Move: function(credentials, datasetID, callback){
		
		var commands = require("./node_modules/nqm-json-import/lib/commands.js");
		
		var self = this;
	
		var cost_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + datasetID.cost_datasetId + '/data?opts={"limit":1000000}'
		};
		
		var tonnage_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + datasetID.tonnage_datasetId + '/data?opts={"limit":1000000}'
		};	
		
		var aCost;
		var aTonnage;		
		
		self.nqm_tbx_query(cost_options, function(aCost){	
			self.nqm_tbx_query(tonnage_options, function(aTonnage){
				
				var data = self.get_cost_by_HWRC_Waste_Contract_Move_data(aCost, aTonnage);
				console.log(data.length)
			
				var commandHost = "https://cmd.nqminds.com";					
				commands.getAccessToken(commandHost, credentials, function(err, accessToken){	
					commands.upsertDatasetDataBulk(commandHost, accessToken, datasetID.output_datasetId, data, function(res){	
						console.log(res)
						callback(res)					
					})
				});	
			});	
		})
	
	},
	
	
	
	generate_NID_permutations: function(subNID, credentials, datasetID, callback){
		
		var commands = require("./node_modules/nqm-json-import/lib/commands.js");
		
		var self = this;
		
		var aSubNID = subNID.split("")
		var flex_count = subNID.split("-").length - 1;
		var max_bit = (Math.pow(2, flex_count) - 1).toString(2).length
		var prefix = Array(max_bit).join("0");
		
		permutation_set = [];
		
		for(var perm_index = 0; perm_index < Math.pow(2, flex_count); perm_index++){
			var perm = (prefix + (perm_index).toString(2)).slice(-max_bit);
			var aPerm = perm.split("");
			
			var NID = subNID;
			
			for(bit_index in aPerm){
				NID = NID.replace("-", aPerm[bit_index]);
			};
		
			permutation_set.push(NID);
			
		};
		
		data = [{ 
			"permutation_set_id": "P1", //always use the same permutation_set_id if you want to overwrite existing set
			"permutation_set": permutation_set
		}];
		
		console.log(data)
		
		var commandHost = "https://cmd.nqminds.com";					
		commands.getAccessToken(commandHost, credentials, function(err, accessToken){	
			commands.upsertDatasetDataBulk(commandHost, accessToken, datasetID.nid_datasetId, data, function(res){	
				console.log(res)
				callback(res)					
			})
		});	
		
	},
	
	next_nid: function(aNID, credentials, datasetID, qCache, callback){
		
		var self = this;
		
		if(aNID.length > 0){
			NID = aNID[aNID.length - 1];	
			
		
			self.tonnage_by_HWRC_Waste_Contract_Move(NID, credentials, datasetID, qCache, function(nid, qCache){
				var index = aNID.indexOf(nid);
				if (index > -1) {aNID.splice(index, 1);}
				console.log("Permutations left to run: " + aNID.length)
				self.next_nid(aNID, credentials, datasetID, qCache, function(res){})
			});
		} else {
			callback("all tonnage permutations calculated and imported")
		};
	},
	
	run_NID_permutations: function(credentials, datasetID, callback){
		
		var self = this;
		
		//used to cache query data		
		var qCache = {};		
		qCache.aRatio = [];
		qCache.aBase = [];
				
		var nid_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + datasetID.nid_datasetId + '/data?opts={"limit":1000000}'
		};
		
		self.nqm_tbx_query(nid_options, function(res){			
			var aNID = res[0].permutation_set;	
			
			self.next_nid(aNID, credentials, datasetID, qCache, function(res){
				console.log(res)
			});					
		})
		
	}
		
}





