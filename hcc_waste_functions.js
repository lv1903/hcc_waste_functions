
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
	
	
	getToken: function(commandHost, credentials, cb) {
		
		var util = require('util');
		var request = require('request');
		
		var url = util.format("%s/token", commandHost);
		request({ url: url, method: "post", headers: { "authorization": "Basic " + credentials }, json: true, body: { grant_type: "client_credentials" } }, function(err, response, content) {
		  cb(null, response.body.access_token);
		});
	}, 

	addDataBulk: function(commandHost, accessToken, datasetId, data, cb) {
		
		var util = require('util');
		var request = require('request');
		
		var url = util.format("%s/commandSync/dataset/data/createMany", commandHost);
		var bulk = {};
		bulk.datasetId = datasetId;
		bulk.payload = data;
		var requestOptions =  { url: url, timeout: 3600000, method: "post",  headers: { authorization: "Bearer " + accessToken }, json: true, body: bulk };
		// console.log("sending createMany [%d - %d bytes] to %s using token %s",data.length, JSON.stringify(data).length, url, accessToken);
		request(requestOptions, function(err, response, content) {
			console.log(err)
			cb(null);		
		});
	}, 
	
	upsertDataBulk: function(commandHost, accessToken, datasetId, data, cb) {
		
		var util = require('util');
		var request = require('request');
		
		var url = util.format("%s/commandSync/dataset/data/upsertMany", commandHost);
		var bulk = {};
		bulk.datasetId = datasetId;
		bulk.payload = data;
		var requestOptions =  { url: url, timeout: 3600000, method: "post",  headers: { authorization: "Bearer " + accessToken }, json: true, body: bulk };
		// console.log("sending upsertMany [%d - %d bytes] to %s using token %s",data.length, JSON.stringify(data).length, url, accessToken);
		request(requestOptions, function(err, response, content) {
			console.log(err)
			cb(null);		
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
						
		for(var sid_index in aSID){					
			var sid = aSID[sid_index];					
			for(var ratio_index in aRatio){						
				var ratio = aRatio[ratio_index];						
				var hwrc = ratio["HWRC"];
				var waste = ratio["Waste_Type"];						
				//extract the tonnage that matches the ratio record
				for(behaviour_index in aBehaviour){							
					var o = aBehaviour[behaviour_index]._id;							
					if(o.HWRC==hwrc){
						if(o.Waste_Type==waste){
							if(o.SID==sid){
								var net_demand = aBehaviour[behaviour_index].net_demand;
								break
							}									
						}
					} 
				}						
				var oRecord = {};
				var Tonnage;
				if(net_demand > 0){
					Tonnage = ratio["ratio"] * net_demand;
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
			
			}
		}
		return data
		
	},

	
	tonnage_by_HWRC_Waste_Contract_Move: function(NID, credentials, output_datasetId, ratio_datasetId, behaviour_datasetId, callback){
		
		var self = this;
	
		var ratio_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + ratio_datasetId + '/data?opts={"limit":1000000}'
		};		
		var behaviour_options = {
			host: 'q.nqminds.com',
			path: self.create_behaviour_path(NID, behaviour_datasetId)
		};		
		
		var aRatio;
		var aBehaviour;
		
		self.nqm_tbx_query(ratio_options, function(aRatio){	
			self.nqm_tbx_query(behaviour_options, function(aBehaviour){
				
				var data = self.get_tonnage_by_HWRC_Waste_Contract_Move_data(NID, aRatio, aBehaviour);
				
				console.log(data.length)
			
				var commandHost = "https://cmd.nqminds.com";
				
				self.getToken(commandHost, credentials, function(err, accessToken){	
					self.upsertDataBulk(commandHost, accessToken, output_datasetId, data, function(res){	
						callback(NID)					
					})
				});	
			});	
		})
	
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
								} else {
									Cost = 0;
								}
								
								oRecord.NID = t.NID
								oRecord.SID = t.SID
								oRecord.HWRC = t.HWRC;
								oRecord.Waste_Type = t.Waste_Type;
								oRecord.Contract = t.Contract;
								oRecord.First_Movement = t.First_Movement;
								oRecord.Tonnage = t.Tonnage;
								oRecord.Cost = Cost;
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
	
	
	cost_by_HWRC_Waste_Contract_Move: function(credentials, output_datasetId, cost_datasetId, tonnage_datasetId, callback){
		
		var self = this;
	
		var cost_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + cost_datasetId + '/data?opts={"limit":1000000}'
		};
		
		var tonnage_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + tonnage_datasetId + '/data?opts={"limit":1000000}'
		};	

			console.log(tonnage_options)
		
		var aCost;
		var aTonnage;		
		
		self.nqm_tbx_query(cost_options, function(aCost){	
			self.nqm_tbx_query(tonnage_options, function(aTonnage){
				
				var data = self.get_cost_by_HWRC_Waste_Contract_Move_data(aCost, aTonnage);
				console.log(data.length)
			
				var commandHost = "https://cmd.nqminds.com";
				
				self.getToken(commandHost, credentials, function(err, accessToken){	
					self.upsertDataBulk(commandHost, accessToken, output_datasetId, data, function(res){	
						callback(res)					
					})
				});	
			});	
		})
	
	},
	
	generate_NID_permutations: function(subNID, credentials, nid_datasetId, callback){
		
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
		
		self.getToken(commandHost, credentials, function(err, accessToken){	
			self.upsertDataBulk(commandHost, accessToken, nid_datasetId, data, function(res){	
				callback(res)					
			});
		});	
		
	},
	
	next_nid: function(aNID, credentials, tonnage_datasetId, ratio_datasetId, behaviour_datasetId, cb){
		
		var self = this;
		
		if(aNID.length > 0){
			NID = aNID[0];	
			self.tonnage_by_HWRC_Waste_Contract_Move(NID, credentials, tonnage_datasetId, ratio_datasetId, behaviour_datasetId, function(nid){
				var index = aNID.indexOf(nid);
				if (index > -1) {aNID.splice(index, 1);}
				console.log(aNID.length)
				self.next_nid(aNID, credentials, tonnage_datasetId, ratio_datasetId, behaviour_datasetId)
			});
		} else {
			callback("all tonnage permutations calculated and imported")
		}
		
		
	},
	
	run_NID_permutations: function(credentials, nid_datasetId, tonnage_datasetId, ratio_datasetId, behaviour_datasetId, callback){
		
		var self = this;		
		var nid_options = {
			host: 'q.nqminds.com',
			path: '/v1/datasets/' + nid_datasetId + '/data?opts={"limit":1000000}'
		};
		
		self.nqm_tbx_query(nid_options, function(res){			
			var aNID = res[0].permutation_set;	
			self.next_nid(aNID, credentials, tonnage_datasetId, ratio_datasetId, behaviour_datasetId, function(res){
				console.log(res)
			});					
		})
		
	}
		
}





