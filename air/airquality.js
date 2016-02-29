var xls = require('xlsjs');
var async = require('async');
var path = require('path');
var elasticsearch = require('elasticsearch');
var md5 = require('MD5');
var fs = require('fs');
var lineReader = require('line-reader');
var stations=require('./stations.js');
var components=require('./airComponents.js');

var es_client = new elasticsearch.Client({
	host:"http://localhost:9200",
	requestTimeout: 60000
});

if (process.argv.length!=3)
{
	console.log("usage: node parse.js file");
	process.exit();
}


var objects = [];

var fileNameProperties = {};

var fileName=process.argv[2];

async.series({
    one: function(callback){
        doParseFileName(callback);
    },    
    two: function(callback){
       doParseFile(callback);
    },
    uploadObjects: function(callback){
       doUploadResults(objects,callback);
    }
},function (err)
{
	if (err)
	{
		console.log("Error: "+err.message);
	}
	else
		console.log("File "+fileName + " parsed succesfully");
	process.exit();
});

function doParseFileName(callback)
{
	//RO0001A0000100100day.1-1-2001.31-12-2005
	var fileNameTmp=path.basename(fileName);
	var parts=fileNameTmp.split('.');

	var startDateParts=parts[1].trim().split('-');
	var endDateParts=parts[2].trim().split('-');

	var stationCode=parts[0].substr(0,7);
	var componentCode=parts[0].substr(7,5).replace(/^0+/, '');

	var measurementType=parts[0].substr(parts[0].length-3,3);

	fileNameProperties.stationCode=stationCode;
	fileNameProperties.componentCode=componentCode;
	fileNameProperties.measurementType=measurementType;

	var startDate = new Date(startDateParts[2],startDateParts[1],startDateParts[0]);
	var endDate = new Date(endDateParts[2],endDateParts[1],endDateParts[0]);
	fileNameProperties.startFileDate=startDate.toISOString();
	fileNameProperties.endFileDate=endDate.toISOString();
//	fileNameProperties.period=endDate.getTime()-startDate.getTime();

	if (stations.stations[fileNameProperties.stationCode])
	{
		var station=stations.stations[fileNameProperties.stationCode];
		for (var key in station)
		{
			fileNameProperties[key]=station[key];
		}
	}

	if (components.components[fileNameProperties.componentCode])
	{
		var comp=components.components[fileNameProperties.componentCode];
		for (var key in comp)
		{
			fileNameProperties[key]=comp[key];
		}
	}

	if (measurementType!="day")
		callback(new Error("not a daily measurement"));
	callback();
}

function doParseFile(callback)
{

	lineReader.eachLine(fileName, function(line, last) {

		var parts=line.split(/\s+/);
		if (parts.length!=0)
		{
			var date = new Date(parts[0]).getTime();
			for (var i=1;i<parts.length/2;i++)
			{
				if(parts[2*i]=="1")
					addNewObservation(date,i-1,parts[2*i-1]);
			}
		}
		if(last)
		{
			callback();
		}
	});
}

function isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function addNewObservation(firstDate,nrObservation,value)
{
	var bulkOp= { "index" : { "_index" : "airquality", "_type" : "observation"} };
	var jsonObj = {
		startDate: new Date(firstDate+nrObservation*24*60*60*1000).toISOString(),
		value:parseFloat(value)
	};
	for (var key in fileNameProperties)
	{
		jsonObj[key]=fileNameProperties[key];
	}
	
	objects.push(bulkOp);
	objects.push(jsonObj);
}

function doUploadResults(bulkObjects,callback)
{
	//console.log(JSON.stringify(bulkObjects));
	
	if (bulkObjects.length==0)
	{
		callback(null);
		return;
	}
	es_client.bulk({body:bulkObjects},callback);

}
