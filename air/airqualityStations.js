var Converter=require("csvtojson").core.Converter;
var fs=require("fs");
var md5 = require('MD5');

var elasticsearch = require('elasticsearch');

var es_client = new elasticsearch.Client({
	host:"http://localhost:9200",
	requestTimeout: 60000
});
 
var csvFileName="./stations.csv";
var fileStream=fs.createReadStream(csvFileName);
//new converter instance 
var csvConverter=new Converter({constructResult:true});
 
//end_parsed will be emitted once parsing finished 
csvConverter.on("end_parsed",function(jsonObj){
   doWriteResults(jsonObj,function(){
   	process.exit();
   })
});
 
//read from file 
fileStream.pipe(csvConverter);


function doUploadResults(jsonObj,callback)
{
	var arrayStations=[]
	for (var i=0;i<jsonObj.length;i++)
	{
		var bulkOp= { "index" : { "_index" : "extrainfo", "_type" : "airQualityStations" , "_id": md5(jsonObj[i].station_european_code)}};
		arrayStations.push(bulkOp);
		arrayStations.push(jsonObj[i]);
	}
	es_client.bulk({body:arrayStations},callback);
}

function doWriteResults(jsonObj,callback)
{
	var arrayStations={};
	for (var i=0;i<jsonObj.length;i++)
	{
		var lon=jsonObj[i].station_longitude_deg;
		var lat=jsonObj[i].station_latitude_deg;
		var name=jsonObj[i].station_name;
		var city=jsonObj[i].station_city;
		var object={
			station_name:name,
			station_city:city,
			stationGeoPoint: []
		};
		object.stationGeoPoint.push(lon);
		object.stationGeoPoint.push(lat);
		arrayStations[jsonObj[i].station_european_code]=object;
	}
	console.log(arrayStations);
	callback(null);
}

