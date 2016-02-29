var elasticsearch = require('elasticsearch');

var es_client = new elasticsearch.Client({
	host:"http://localhost:9200",
	requestTimeout: 60000
});
var hospitals=require('./hospitals.js');

var objects=[];
function doRowWork(jsonObject)
{
	var bulkOp= { "index" : { "_index" : "extrainfo", "_type" : "hospitals" } };
	for (var key in hospitals.hospitals)
	{
		objects.push(bulkOp);
		objects.push(hospitals.hospitals[key]);
	}
	
}

function doUploadResults(callback)
{
	if (objects.length==0)
	{
		callback(null);
		return;
	}
	es_client.bulk({body:objects},callback);
}

doRowWork();
doUploadResults(function(){process.exit()});